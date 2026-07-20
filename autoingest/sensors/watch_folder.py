import os
import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.ingest_jobs import ingest_local_job
from autoingest.resources.utils import accepted_file_type


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

    cursor_files = set()
    if context.cursor:
        try:
            cursor_files = set(json.loads(context.cursor))
        except (json.JSONDecodeError, TypeError):
            cursor_files = set()

    context.log.info(
        f"Sensor tick — {len(watch_paths)} watch folder(s), "
        f"{len(cursor_files)} files in cursor"
    )

    retryable = set()
    if cursor_files:
        db = context.resources.workflow_db
        try:
            non_retryable = db.get_non_retryable_cursor_files(cursor_files)
            retryable = cursor_files - non_retryable
        except Exception as exc:
            context.log.warning(f"DB query for cursor cleanup failed, skipping: {exc}")
            retryable = set()

    if retryable:
        context.log.info(
            f"Removing {len(retryable)} retryable files from cursor "
            f"(no DB row or status='No Status')"
        )
        for f in sorted(retryable):
            context.log.info(f"  Re-scanning: {Path(f).name}")

    seen_files = cursor_files - retryable

    TICK_DEADLINE_SEC = 55

    new_files = []
    current_files = set()
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

        ingest_folders = [x for x in os.listdir(watch_dir) if os.path.isdir(os.path.join(watch_dir, x))]
        for folder in ingest_folders:
            folder_path = os.path.join(watch_dir, folder)
            files = [x for x in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, x))]
            for file in files:
                file_key = os.path.join(folder_path, file)
                file_path = Path(file_key)
                total_scanned += 1

                if time.perf_counter() - tick_start > TICK_DEADLINE_SEC:
                    timed_out = True
                    break
                if not accepted_file_type(file_path.suffix.lstrip(".")):
                    skipped_extension += 1
                    continue

                current_files.add(file_key)

                if file_key in seen_files:
                    continue

                try:
                    st = file_path.stat()
                    age_sec = time.time() - st.st_mtime
                    if age_sec < 5 or st.st_size == 0:
                        skipped_size += 1
                        context.log.info(
                            f"Skipping in-flight file: {file_path.name} "
                            f"(modified {age_sec:.1f}s ago, size={st.st_size})"
                        )
                        continue
                except OSError as exc:
                    context.log.warning(f"OS error checking {file_path.name}: {exc}")
                    continue

                context.log.info(f"New file detected: {file_path.name} ({st.st_size} bytes)")
                new_files.append(file_key)

        if timed_out:
            context.log.warning(
                f"Tick time limit ({TICK_DEADLINE_SEC}s) reached — "
                f"{total_scanned} items scanned, remaining files deferred to next tick"
            )
            break

    context.log.info(
        f"Scan complete — scanned {total_scanned}, "
        f"skipped: not-file={skipped_not_file} ext={skipped_extension} "
        f"size-unstable={skipped_size}, new={len(new_files)}"
    )

    run_requests = []
    for file_key in new_files:
        run_requests.append(
            RunRequest(
                run_key=f"ingest-{Path(file_key).name}-{int(time.time())}",
                run_config={
                    "ops": {
                        "assess_filename": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )

    updated_seen = list(current_files)
    context.update_cursor(json.dumps(updated_seen))
    context.log.info(f"Cursor updated: {len(updated_seen)} files tracked")
    return run_requests
