import os
import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.validation_jobs import verify_local_job


RETRY_INTERVAL_SECONDS = 300
MAX_QUEUED_PER_STAGE = 80
MAX_NEW_PER_TICK = 50
ACTIVE_LIMIT = 80
ACTIVE_GATE_STATUSES = ("validating", "verified")


@sensor(
    job=verify_local_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def validation_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    validate_paths = os.environ.get("VALIDATION_FOLDER_PATHS", "").split(",")
    validate_paths = [p.strip() for p in validate_paths if p.strip()]

    if not validate_paths:
        context.log.warning(
            "VALIDATION_FOLDER_PATHS is empty — no folders to watch."
        )
        return []

    db = context.resources.workflow_db
    now = int(time.time())

    # ── Phase 1: Scan validation directories ─────────────────
    # Always scan, regardless of pipeline depth.

    current_files: dict[str, str] = {}
    total_subdirs = 0
    skipped_ingest = 0
    skipped_not_dir = 0
    skipped_not_file = 0

    for validate_path in validate_paths:
        validate_dir = Path(validate_path)
        if not validate_dir.exists():
            context.log.warning(
                f"Validation folder does not exist: {validate_path}"
            )
            continue

        for subfolder in validate_dir.iterdir():
            total_subdirs += 1

            if not subfolder.is_dir():
                skipped_not_dir += 1
                continue
            if subfolder.name.startswith("ingest_"):
                skipped_ingest += 1
                context.log.info(
                    f"Skipping incomplete ingest folder: {subfolder.name}"
                )
                continue

            for file_path in subfolder.iterdir():
                if not file_path.is_file():
                    skipped_not_file += 1
                    continue

                file_key = str(file_path)
                current_files[file_key] = file_path.name

    context.log.info(
        f"Scan complete — subdirs={total_subdirs}, "
        f"skipped: not-dir={skipped_not_dir} ingest={skipped_ingest} "
        f"not-file={skipped_not_file}, found={len(current_files)}"
    )

    # ── Phase 2: Load and prune cursor ───────────────────────
    # Cursor: {path: timestamp} — prevents re-launch within
    # RETRY_INTERVAL_SECONDS and enables crash recovery retry.

    cursor: dict[str, int] = {}
    if context.cursor:
        try:
            raw = json.loads(context.cursor)
            if isinstance(raw, dict):
                cursor = {str(k): int(v) for k, v in raw.items()}
            elif isinstance(raw, list):
                cursor = {str(v): 0 for v in raw}
        except (json.JSONDecodeError, TypeError, ValueError):
            cursor = {}

    cursor_initial = len(cursor)

    # Prune: remove paths for files no longer on disk (deleted
    # by cleanup step after successful verification+encoding).
    stale_on_disk = {fp for fp in cursor if fp not in current_files}
    for fp in stale_on_disk:
        del cursor[fp]
    if stale_on_disk:
        context.log.info(
            f"Cursor: removed {len(stale_on_disk)} paths no longer on disk"
        )

    # ── Phase 3: DB batch lookup ─────────────────────────────

    all_disk_paths = set(current_files.keys())
    lookup_paths = all_disk_paths | set(cursor.keys())

    existing_statuses: dict[str, str] = {}
    try:
        if lookup_paths:
            existing_statuses = db.batch_lookup_file_statuses(lookup_paths)
    except Exception as exc:
        context.log.warning(
            f"DB batch lookup failed, proceeding with empty lookup: {exc}"
        )

    # Prune cursor: remove paths where DB status has moved past
    # the verify stage (no longer "File cleared for ingest" or
    # "validating"). Files at "validating" stay to enable crash
    # recovery retry.
    POST_VERIFY = {"File cleared for ingest", "validating"}
    cursor_with_db = {
        fp for fp in cursor
        if fp in existing_statuses
        and existing_statuses[fp] not in POST_VERIFY
    }
    for fp in cursor_with_db:
        del cursor[fp]
    if cursor_with_db:
        context.log.info(
            f"Cursor: removed {len(cursor_with_db)} paths past verify stage"
        )

    context.log.info(
        f"Cursor state: {len(cursor)} pending "
        f"(initial={cursor_initial}, pruned_disk={len(stale_on_disk)}, "
        f"pruned_db={len(cursor_with_db)})"
    )

    # ── Phase 4: Active gate ─────────────────────────────────
    # Count files already in the validation pipeline and skip
    # launching if at or above the limit.

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM app.file_catalogue "
                    "WHERE file_status IN %s",
                    (ACTIVE_GATE_STATUSES,),
                )
                active_count = cur.fetchone()[0]
    except Exception as exc:
        context.log.warning(
            f"Active-gate check failed, skipping new launches: {exc}"
        )
        active_count = ACTIVE_LIMIT + 1

    if active_count >= ACTIVE_LIMIT:
        context.log.info(
            f"validation_folder_sensor: skipping tick — "
            f"{active_count} files already active "
            f"{ACTIVE_GATE_STATUSES}, limit={ACTIVE_LIMIT}"
        )
        context.update_cursor(json.dumps(cursor))
        return []

    context.log.info(
        f"validation_folder_sensor: active gate OK — "
        f"{active_count} active (limit {ACTIVE_LIMIT})"
    )

    # ── Phase 5: Queue gate ──────────────────────────────────
    # Count files waiting at "File cleared for ingest" status
    # and skip launching if above the queue limit.

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM app.file_catalogue "
                    "WHERE file_status = 'File cleared for ingest'"
                )
                queued_count = cur.fetchone()[0]
    except Exception as exc:
        context.log.warning(
            f"Queue-depth check failed, skipping new launches: {exc}"
        )
        queued_count = MAX_QUEUED_PER_STAGE + 1

    if queued_count > MAX_QUEUED_PER_STAGE:
        context.log.info(
            f"validation_folder_sensor: skipping tick — "
            f"{queued_count} files queued for validation, "
            f"exceeds limit of {MAX_QUEUED_PER_STAGE}"
        )
        context.update_cursor(json.dumps(cursor))
        return []

    context.log.info(
        f"validation_folder_sensor: queue depth OK — "
        f"{queued_count} files (limit {MAX_QUEUED_PER_STAGE})"
    )

    # ── Phase 6: Candidate filter ────────────────────────────
    # A file triggers verification iff:
    #   · Not launched within RETRY_INTERVAL_SECONDS, AND
    #   · DB status is "File cleared for ingest" (new), OR
    #   · DB status is "validating" and we previously launched it
    #     (crash recovery — stuck at validating)

    candidates = []
    skipped_retry = 0
    skipped_not_ready = 0

    for file_key, file_name in current_files.items():
        last_launch = cursor.get(file_key)

        if last_launch is not None:
            if (now - last_launch) < RETRY_INTERVAL_SECONDS:
                skipped_retry += 1
                continue

        db_status = existing_statuses.get(file_key)

        if db_status == "File cleared for ingest":
            candidates.append(file_key)
        elif db_status == "validating":
            if last_launch is not None:
                context.log.info(
                    f"Retrying {file_name} — stuck at 'validating' for "
                    f"{now - last_launch}s"
                )
                candidates.append(file_key)
            else:
                skipped_not_ready += 1
        elif db_status is None:
            skipped_not_ready += 1
            context.log.info(
                f"Skipping {file_name} — no DB record found"
            )
        else:
            skipped_not_ready += 1
            context.log.info(
                f"Skipping {file_name} — status is '{db_status}', "
                f"expected 'File cleared for ingest'"
            )

    context.log.info(
        f"Dedup: {len(candidates)} candidates, "
        f"skipped: retry-interval={skipped_retry} "
        f"not-ready={skipped_not_ready}"
    )

    # ── Phase 7: Launch RunRequests ───────────────────────────

    new_requests = []
    launched = 0
    for file_key in candidates:
        if launched >= MAX_NEW_PER_TICK:
            context.log.info(
                f"Per-tick launch cap ({MAX_NEW_PER_TICK}) reached — "
                f"{len(candidates) - launched} candidates deferred"
            )
            break
        file_name = current_files[file_key]
        context.log.info(
            f"Validation sensor: launching verify for {file_name}"
        )
        new_requests.append(
            RunRequest(
                run_key=f"validate-{file_name}-{now}",
                run_config={
                    "ops": {
                        "verify_tape_copy": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )
        cursor[file_key] = now
        launched += 1

    # ── Phase 8: Update cursor ────────────────────────────────

    context.update_cursor(json.dumps(cursor))
    context.log.info(
        f"Cursor updated — {launched} launched, "
        f"{len(cursor)} pending, "
        f"{len(current_files)} files on disk"
    )
    return new_requests
