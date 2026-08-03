import json
from datetime import datetime, timezone
from pathlib import Path
import time

from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.validation_jobs import verify_local_job
from autoingest.ops.local.verification import retrieve_json_data


MAX_QUEUED_PER_STAGE = 30
MAX_NEW_PER_TICK = 50
ACTIVE_LIMIT = 40
ACTIVE_GATE_STATUSES = ("validating",)
CANDIDATE_LIMIT = 1000


@sensor(
    job=verify_local_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def validation_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    db = context.resources.workflow_db

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

    # ── Phase 2: Load cursor ─────────────────────────────────
    # New format: {"submitted": [...], "json_checked": {...}}
    # Old format (upgraded): list of paths or dict of {path: timestamp}

    submitted_paths: set[str] = set()
    json_checked: dict[str, str] = {}
    if context.cursor:
        try:
            raw = json.loads(context.cursor)
            if isinstance(raw, list):
                submitted_paths = {str(v) for v in raw}
            elif isinstance(raw, dict):
                if "submitted" in raw:
                    submitted_paths = {str(v) for v in raw["submitted"]}
                    json_checked = raw.get("json_checked", {})
                else:
                    submitted_paths = {str(k) for k in raw}
        except (json.JSONDecodeError, TypeError, ValueError):
            submitted_paths = set()
            json_checked = {}

    cursor_initial = len(submitted_paths)

    # ── Phase 2.5: Resolve bp_json_pending files ─────────────
    # Files previously blocked because the Black Pearl notification
    # JSON had not yet been written. Re-check on disk and move
    # back to "File cleared for ingest" when the JSON appears.

    now = datetime.now(timezone.utc)
    JSON_BACKOFF_SEC = 600

    resolved_count = 0
    skipped_backoff = 0
    still_pending = 0

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, file_name, file_path, bp_job_id "
                    "FROM app.file_catalogue "
                    "WHERE file_status = 'bp_json_pending' "
                    "  AND bp_job_id IS NOT NULL AND bp_job_id != '' "
                    "ORDER BY created_at ASC "
                    "LIMIT %s",
                    (CANDIDATE_LIMIT,),
                )
                pending_rows = cur.fetchall()
    except Exception as exc:
        context.log.warning(
            f"bp_json_pending query failed, skipping resolution: {exc}"
        )
        pending_rows = []

    for file_id, file_name, file_path, bp_job_id in pending_rows:
        validation_path = (
            Path(file_path).parent.parent.parent.parent
            / "autoingest" / "validation" / bp_job_id / file_name
        )
        file_key = str(validation_path)

        last_check_str = json_checked.get(file_key)
        if last_check_str:
            try:
                last_check = datetime.fromisoformat(last_check_str)
                if (now - last_check).total_seconds() < JSON_BACKOFF_SEC:
                    skipped_backoff += 1
                    continue
            except ValueError:
                pass

        json_checked[file_key] = now.isoformat()
        json_path = retrieve_json_data(bp_job_id.strip())
        if json_path:
            db.update_file_status(
                file_id, file_status="File cleared for ingest", error_message=""
            )
            submitted_paths.discard(file_key)
            json_checked.pop(file_key, None)
            resolved_count += 1
        else:
            still_pending += 1

    if resolved_count or still_pending:
        context.log.info(
            f"bp_json_pending: resolved={resolved_count}, "
            f"still_pending={still_pending}, "
            f"deferred_backoff={skipped_backoff}"
        )

    # ── Phase 3: Prune cursor ────────────────────────────────

    stale_on_disk = submitted_paths - set(current_files.keys())
    submitted_paths -= stale_on_disk
    if stale_on_disk:
        context.log.info(
            f"Cursor: removed {len(stale_on_disk)} paths no longer on disk"
        )

    context.log.info(
        f"Cursor state: {len(submitted_paths)} pending "
        f"(initial={cursor_initial}, pruned_disk={len(stale_on_disk)})"
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
        cursor_data = {
            "submitted": sorted(submitted_paths),
            "json_checked": json_checked,
        }
        context.update_cursor(json.dumps(cursor_data))
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
            f"validation_folder_sensor: drain mode — "
            f"{queued_count} files queued for validation, "
            f"launching at reduced rate"
        )

    context.log.info(
        f"validation_folder_sensor: queue depth — "
        f"{queued_count} files (limit {MAX_QUEUED_PER_STAGE})"
    )

    # ── Phase 6: Candidate filter ────────────────────────────
    # A file triggers verification iff:
    #   · Not already submitted (not in submitted_paths).
    # All files in current_files are at "File cleared for ingest" by
    # definition (they came from the DB query), so no status check needed.

    candidates = []
    skipped_cursor = 0

    for file_key, file_name in current_files.items():
        if file_key in submitted_paths:
            skipped_cursor += 1
            continue

        candidates.append(file_key)

    context.log.info(
        f"Dedup: {len(candidates)} candidates, "
        f"skipped: cursor={skipped_cursor}"
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
                run_key=f"validate-{file_name}-{int(time.time())}",
                run_config={
                    "ops": {
                        "verify_tape_copy": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )
        submitted_paths.add(file_key)
        launched += 1

    # ── Phase 8: Update cursor ────────────────────────────────

    cursor_data = {
        "submitted": sorted(submitted_paths),
        "json_checked": json_checked,
    }
    context.update_cursor(json.dumps(cursor_data))
    context.log.info(
        f"Cursor updated — {launched} launched, "
        f"{len(submitted_paths)} pending, "
        f"{len(current_files)} files on disk, "
        f"{len(json_checked)} json_checked entries"
    )
    return new_requests
