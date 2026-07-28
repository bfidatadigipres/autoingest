import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.validation_jobs import verify_local_job


RETRY_INTERVAL_SECONDS = 300
MAX_QUEUED_PER_STAGE = 30
MAX_NEW_PER_TICK = 50
ACTIVE_LIMIT = 20
ACTIVE_GATE_STATUSES = ("validating", "verified")
CANDIDATE_LIMIT = 1000


@sensor(
    job=verify_local_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def validation_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    db = context.resources.workflow_db
    now = int(time.time())

    # ── Phase 1: DB-driven candidate discovery ─────────────────
    # Query the database for files ready for verification,
    # reconstruct the validation path from stored fields.
    # No filesystem checks — the verify_tape_copy op validates
    # on-disk existence at runtime.

    current_files: dict[str, str] = {}
    db_rows: list[tuple[str, str, str]] = []

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_name, file_path, bp_job_id "
                    "FROM app.file_catalogue "
                    "WHERE file_status = 'File cleared for ingest' "
                    "  AND bp_job_id IS NOT NULL "
                    "  AND bp_job_id != '' "
                    "ORDER BY created_at ASC "
                    "LIMIT %s",
                    (CANDIDATE_LIMIT,),
                )
                db_rows = cur.fetchall()
    except Exception as exc:
        context.log.warning(
            f"DB candidate query failed, returning empty: {exc}"
        )
        return []

    for file_name, file_path, bp_job_id in db_rows:
        validation_path = (
            Path(file_path).parent.parent.parent.parent
            / "autoingest" / "validation" / bp_job_id / file_name
        )
        file_key = str(validation_path)
        current_files[file_key] = file_name

    context.log.info(
        f"DB query returned {len(db_rows)} candidates, "
        f"loaded {len(current_files)} paths"
    )

    # ── Phase 2: Load and prune cursor ───────────────────────
    # Cursor: {path: timestamp} — prevents re-launch within
    # RETRY_INTERVAL_SECONDS.

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

    # Prune: remove paths for files no longer on disk
    # (deleted by cleanup step after successful verification+encoding).
    stale_on_disk = {fp for fp in cursor if fp not in current_files}
    for fp in stale_on_disk:
        del cursor[fp]
    if stale_on_disk:
        context.log.info(
            f"Cursor: removed {len(stale_on_disk)} paths no longer on disk"
        )

    context.log.info(
        f"Cursor state: {len(cursor)} pending "
        f"(initial={cursor_initial}, pruned_disk={len(stale_on_disk)})"
    )

    # ── Phase 3: Active gate ─────────────────────────────────
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

    # ── Phase 4: Queue gate ──────────────────────────────────
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
            f"validation_folder_sensor: drain mode — "
            f"{queued_count} files queued for validation, "
            f"launching at reduced rate"
        )

    context.log.info(
        f"validation_folder_sensor: queue depth — "
        f"{queued_count} files (limit {MAX_QUEUED_PER_STAGE})"
    )

    # ── Phase 5: Candidate filter ────────────────────────────
    # A file triggers verification iff:
    #   · Not launched within RETRY_INTERVAL_SECONDS.
    # All files in current_files are at "File cleared for ingest" by
    # definition (they came from the DB query), so no status check needed.

    candidates = []
    skipped_retry = 0

    for file_key, file_name in current_files.items():
        last_launch = cursor.get(file_key)

        if last_launch is not None:
            if (now - last_launch) < RETRY_INTERVAL_SECONDS:
                skipped_retry += 1
                continue

        candidates.append(file_key)

    context.log.info(
        f"Dedup: {len(candidates)} candidates, "
        f"skipped: retry-interval={skipped_retry}"
    )

    # ── Phase 6: Launch RunRequests ───────────────────────────

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

    # ── Phase 7: Update cursor ────────────────────────────────

    context.update_cursor(json.dumps(cursor))
    context.log.info(
        f"Cursor updated — {launched} launched, "
        f"{len(cursor)} pending, "
        f"{len(current_files)} files on disk"
    )
    return new_requests
