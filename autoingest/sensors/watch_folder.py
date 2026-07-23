import os
import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.ingest_jobs import ingest_local_job
from autoingest.resources.utils import accepted_file_type


MAX_INGEST_DEPTH = 200
MAX_NEW_PER_TICK = 50
TICK_DEADLINE_SEC = 300
RETRYABLE_STATUSES = {"No Status", "Failed assessment"}


@sensor(
    job=ingest_local_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def watch_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    tick_start = time.perf_counter()

    watch_paths = os.environ.get("WATCH_FOLDER_PATHS", "").split(",")
    watch_paths = [p.strip() for p in watch_paths if p.strip()]

    if not watch_paths:
        context.log.warning("WATCH_FOLDER_PATHS is empty — no folders to watch.")
        return []

    db = context.resources.workflow_db

    # ── Phase 1: Scan watch directories ──────────────────────
    # Always scan, regardless of pipeline depth.

    current_files: dict[str, int] = {}
    total_scanned = 0
    skipped_extension = 0
    skipped_not_file = 0
    skipped_size = 0
    timed_out = False

    for watch_path in watch_paths:
        watch_dir = Path(watch_path)
        if not watch_dir.exists():
            context.log.warning(f"Watch folder does not exist: {watch_path}")
            continue

        try:
            ingest_folders = [
                x for x in os.listdir(watch_dir)
                if os.path.isdir(os.path.join(watch_dir, x))
            ]
        except OSError:
            continue

        for folder in ingest_folders:
            folder_path = os.path.join(watch_dir, folder)
            try:
                folder_items = os.listdir(folder_path)
            except OSError:
                continue

            for file in folder_items:
                file_key = os.path.join(folder_path, file)
                file_path_obj = Path(file_key)
                total_scanned += 1

                if time.perf_counter() - tick_start > TICK_DEADLINE_SEC:
                    timed_out = True
                    break

                if not file_path_obj.is_file():
                    skipped_not_file += 1
                    continue
                if not accepted_file_type(file_path_obj.suffix.lstrip(".")):
                    skipped_extension += 1
                    continue

                try:
                    st = file_path_obj.stat()
                    age_sec = time.time() - st.st_mtime
                    if age_sec < 5 or st.st_size == 0:
                        skipped_size += 1
                        context.log.info(
                            f"Skipping in-flight file: {file_path_obj.name} "
                            f"(modified {age_sec:.1f}s ago, size={st.st_size})"
                        )
                        continue
                except OSError as exc:
                    context.log.warning(
                        f"OS error checking {file_path_obj.name}: {exc}"
                    )
                    continue

                current_files[file_key] = st.st_size

            if timed_out:
                break

        if timed_out:
            context.log.warning(
                f"Tick time limit ({TICK_DEADLINE_SEC}s) reached — "
                f"{total_scanned} items scanned, remaining deferred to next tick"
            )
            break

    context.log.info(
        f"Scan complete — scanned {total_scanned}, "
        f"skipped: not-file={skipped_not_file} ext={skipped_extension} "
        f"size-unstable={skipped_size}, found={len(current_files)}"
    )

    # ── Phase 2: DB lookup for deduplication ─────────────────
    # PostgreSQL is the source of truth. A file needs processing iff:
    #   · No DB row exists for its file_path, OR
    #   · Existing row has status in RETRYABLE_STATUSES

    existing_statuses: dict[str, str] = {}
    try:
        existing_statuses = db.batch_lookup_file_statuses(
            set(current_files.keys())
        )
    except Exception as exc:
        context.log.warning(
            f"DB batch lookup failed, proceeding with empty lookup: {exc}"
        )

    candidates = []
    skipped_existing = 0
    for file_key in current_files:
        db_status = existing_statuses.get(file_key)
        if db_status is None:
            candidates.append(file_key)
        elif db_status in RETRYABLE_STATUSES:
            candidates.append(file_key)
            context.log.info(
                f"  Retry candidate: {Path(file_key).name} "
                f"(status={db_status})"
            )
        else:
            skipped_existing += 1

    context.log.info(
        f"DB dedup: {len(candidates)} new/retry candidates, "
        f"{skipped_existing} skipped (already known to DB)"
    )

    # ── Phase 3: Gate check (ingest stages only) ─────────────
    # Only count files in the pre-BP-PUT ingest pipeline so slow
    # encoding/validation never blocks new file discovery.

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM app.file_catalogue "
                    "WHERE file_status IN ("
                    "'No Status', 'assessed', 'checksummed', "
                    "'cataloguing', 'File cleared for ingest'"
                    ")"
                )
                ingest_count = cur.fetchone()[0]
    except Exception as exc:
        context.log.warning(
            f"Ingest-depth check failed, skipping new launches: {exc}"
        )
        ingest_count = MAX_INGEST_DEPTH + 1

    if ingest_count > MAX_INGEST_DEPTH:
        context.log.info(
            f"watch_folder_sensor: skipping new launches — "
            f"{ingest_count} files in ingest pipeline, "
            f"exceeds limit of {MAX_INGEST_DEPTH}"
        )
        context.update_cursor(json.dumps({
            "tick_ts": int(time.time()),
            "files_on_disk": len(current_files),
            "candidates_found": len(candidates),
            "launched": 0,
            "gate_blocked": True,
        }))
        return []

    context.log.info(
        f"watch_folder_sensor: ingest depth OK — "
        f"{ingest_count} files (limit {MAX_INGEST_DEPTH})"
    )

    # ── Phase 4: Launch RunRequests ───────────────────────────

    run_requests = []
    launched = 0
    for file_key in candidates:
        if launched >= MAX_NEW_PER_TICK:
            context.log.info(
                f"Per-tick launch cap ({MAX_NEW_PER_TICK}) reached — "
                f"{len(candidates) - launched} candidates deferred"
            )
            break
        fname = Path(file_key).name
        context.log.info(
            f"New file detected: {fname}"
        )
        run_requests.append(
            RunRequest(
                run_key=f"ingest-{fname}-{int(time.time())}",
                run_config={
                    "ops": {
                        "assess_filename": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )
        launched += 1

    # ── Phase 5: Update cursor (metadata only) ────────────────

    context.update_cursor(json.dumps({
        "tick_ts": int(time.time()),
        "files_on_disk": len(current_files),
        "candidates_found": len(candidates),
        "launched": launched,
        "skipped_existing": skipped_existing,
    }))
    context.log.info(
        f"Cursor updated — {launched} run(s) launched, "
        f"{len(current_files)} files on disk"
    )
    return run_requests
