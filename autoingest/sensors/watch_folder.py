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
)
def watch_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    watch_paths = os.environ.get("WATCH_FOLDER_PATHS", "").split(",")
    watch_paths = [p.strip() for p in watch_paths if p.strip()]

    seen_files = set()
    if context.cursor:
        try:
            seen_files = set(json.loads(context.cursor))
        except (json.JSONDecodeError, TypeError):
            seen_files = set()

    new_files = []
    current_files = set()

    for watch_path in watch_paths:
        watch_dir = Path(watch_path)
        if not watch_dir.exists():
            context.log.warning(f"Watch folder does not exist: {watch_path}")
            continue

        for file_path in watch_dir.rglob("*"):
            if not file_path.is_file():
                continue
            if not accepted_file_type(file_path.suffix.lstrip(".")):
                continue

            file_key = str(file_path)
            current_files.add(file_key)

            if file_key not in seen_files:
                try:
                    size_1 = file_path.stat().st_size
                    time.sleep(2)
                    size_2 = file_path.stat().st_size
                    if size_1 != size_2 or size_1 == 0:
                        continue
                except OSError:
                    continue

                new_files.append(file_key)

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

    updated_seen = list(current_files | set(new_files))
    context.update_cursor(json.dumps(list(updated_seen)))
    return run_requests