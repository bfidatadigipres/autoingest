import os
import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.single_file_ingest import single_file_ingest_job
from autoingest.resources.utils import accepted_file_type


@sensor(
    job=single_file_ingest_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def watch_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
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

    new_files = []
    current_files = set()
    total_scanned = 0
    skipped_extension = 0
    skipped_not_file = 0
    skipped_size = 0

    for watch_path in watch_paths:
        watch_dir = Path(watch_path)
        if not watch_dir.exists():
            context.log.warning(f"Watch folder does not exist: {watch_path}")
            continue

        for file_path in watch_dir.rglob("*"):
            total_scanned += 1

            if not file_path.is_file():
                skipped_not_file += 1
                continue
            if not accepted_file_type(file_path.suffix.lstrip(".")):
                skipped_extension += 1
                continue

            file_key = str(file_path)
            current_files.add(file_key)

            if file_key in seen_files:
                continue

            try:
                size_1 = file_path.stat().st_size
                time.sleep(2)
                size_2 = file_path.stat().st_size
                if size_1 != size_2 or size_1 == 0:
                    skipped_size += 1
                    context.log.info(
                        f"Skipping in-flight file: {file_path.name} "
                        f"(size changed {size_1}→{size_2})"
                    )
                    continue
            except OSError as exc:
                context.log.warning(f"OS error checking {file_path.name}: {exc}")
                continue

            context.log.info(f"New file detected: {file_path.name} ({size_1} bytes)")
            new_files.append(file_key)

    context.log.info(
        f"Scan complete — scanned {total_scanned}, "
        f"skipped: not-file={skipped_not_file} ext={skipped_extension} "
        f"size-unstable={skipped_size}, new={len(new_files)}"
    )

    run_requests = []
    for file_key in new_files:
        run_requests.append(
            RunRequest(
                run_key=f"ingest-{file_key}",
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
