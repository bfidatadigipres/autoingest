import os
import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.ingest_jobs import ingest_local_job
from autoingest.resources.utils import accepted_file_type


MAX_INGEST_DEPTH = 30
MAX_NEW_PER_TICK = 10
TICK_DEADLINE_SEC = 55
RETRYABLE_STATUSES = {"No Status", "Failed assessment"}


@sensor(
    job=ingest_local_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def watch_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    tick_start = time.perf_counter()

    watch_paths_file = os.environ.get("WATCH_PATHS_FILE", "")
    if watch_paths_file:
        try:
            watch_paths = [
                line.strip()
                for line in Path(watch_paths_file).read_text().splitlines()
                if line.strip() and not line.startswith("#")
            ]
        except Exception as exc:
            context.log.warning(f"Failed to read {watch_paths_file}: {exc}")
            watch_paths = []
    else:
        watch_paths = [
            p.strip() for p in os.environ.get("WATCH_FOLDER_PATHS", "").split(",")
            if p.strip()
        ]

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
            with os.scandir(watch_dir) as dir_iter:
                for entry in dir_iter:
                    if not entry.is_dir():
                        continue
                    folder_path = entry.path
                    try:
                        with os.scandir(folder_path) as file_iter:
                            for file_entry in file_iter:
                                total_scanned += 1

                                if time.perf_counter() - tick_start > TICK_DEADLINE_SEC:
                                    timed_out = True
                                    break

                                if not file_entry.is_file():
                                    skipped_not_file += 1
                                    continue
                                if not accepted_file_type(
                                    Path(file_entry.name).suffix.lstrip(".")
                                ):
                                    skipped_extension += 1
                                    continue

                                try:
                                    st = file_entry.stat()
                                    age_sec = time.time() - st.st_mtime
                                    if age_sec < 5 or st.st_size == 0:
                                        skipped_size += 1
                                        context.log.info(
                                            f"Skipping in-flight file: {file_entry.name} "
                                            f"(modified {age_sec:.1f}s ago, size={st.st_size})"
                                        )
                                        continue
                                except OSError as exc:
                                    context.log.warning(
                                        f"OS error checking {file_entry.name}: {exc}"
                                    )
                                    continue

                                current_files[file_entry.path] = st.st_size

                    except OSError:
                        continue

                    if timed_out:
                        break
        except OSError:
            pass

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

    # ── Phase 2: Load and prune cursor ───────────────────────
    # Cursor stores paths that have been launched but not yet
    # confirmed by DB — sensor memory between ticks.

    submitted_paths: set[str] = set()
    if context.cursor:
        try:
            raw = json.loads(context.cursor)
            if isinstance(raw, list):
                submitted_paths = set(raw)
        except (json.JSONDecodeError, TypeError):
            submitted_paths = set()

    cursor_initial = len(submitted_paths)

    # Prune: remove paths for files no longer on disk (moved away
    # by catalogue after processing).
    stale_on_disk = submitted_paths - set(current_files.keys())
    submitted_paths -= stale_on_disk
    if stale_on_disk:
        context.log.info(
            f"Cursor: removed {len(stale_on_disk)} paths no longer on disk"
        )

    # ── Phase 3: DB batch lookup ─────────────────────────────
    # Cross-reference both disk files and cursor entries against
    # DB to determine what truly needs processing.

    all_disk_paths = set(current_files.keys())
    lookup_paths = all_disk_paths | submitted_paths

    existing_statuses: dict[str, str] = {}
    try:
        if lookup_paths:
            existing_statuses = db.batch_lookup_file_statuses(lookup_paths)
    except Exception as exc:
        context.log.warning(
            f"DB batch lookup failed, proceeding with empty lookup: {exc}"
        )

    # Prune cursor: remove paths that now have DB records.
    # This means the op processed the file between the last launch
    # and this tick — the DB is now authoritative for those files.
    cursor_with_db = {p for p in submitted_paths if p in existing_statuses}
    submitted_paths -= cursor_with_db
    if cursor_with_db:
        context.log.info(
            f"Cursor: removed {len(cursor_with_db)} paths now in DB"
        )

    context.log.info(
        f"Cursor state: {len(submitted_paths)} pending "
        f"(initial={cursor_initial}, pruned_disk={len(stale_on_disk)}, "
        f"pruned_db={len(cursor_with_db)})"
    )

    # ── Phase 4: Candidate filter ─────────────────────────────
    # A file needs processing iff:
    #   · NOT already launched (not in submitted_paths), AND
    #   · No DB row, OR DB row with retryable status

    candidates = []
    skipped_cursor = 0
    skipped_db = 0

    for file_key in current_files:
        if file_key in submitted_paths:
            skipped_cursor += 1
            continue

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
            skipped_db += 1

    context.log.info(
        f"Dedup: {len(candidates)} candidates, "
        f"skipped: cursor={skipped_cursor} db={skipped_db}"
    )

    # ── Phase 5: Gate check (ingest stages only) ─────────────
    # Only count files actively in the pre-BP-PUT ingest pipeline.
    # 'File cleared for ingest' is excluded — it means catalogue
    # is done and the file is waiting for external BP PUT.

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM app.file_catalogue "
                    "WHERE file_status IN ("
                    "'No Status', 'assessed', 'checksummed', "
                    "'cataloguing'"
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
            f"watch_folder_sensor: drain mode — "
            f"{ingest_count} files in ingest pipeline, "
            f"launching at reduced rate"
        )

    context.log.info(
        f"watch_folder_sensor: ingest depth — "
        f"{ingest_count} files (limit {MAX_INGEST_DEPTH})"
    )

    # ── Phase 6: Launch RunRequests ───────────────────────────

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
        context.log.info(f"New file detected: {fname}")
        run_requests.append(
            RunRequest(
                run_key=f"ingest-{fname}",
                run_config={
                    "ops": {
                        "assess_filename": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )
        submitted_paths.add(file_key)
        launched += 1

    # ── Phase 7: Update cursor ────────────────────────────────

    context.update_cursor(json.dumps(sorted(submitted_paths)))
    context.log.info(
        f"Cursor updated — {launched} launched, "
        f"{len(submitted_paths)} pending, "
        f"{len(current_files)} files on disk"
    )
    return run_requests
