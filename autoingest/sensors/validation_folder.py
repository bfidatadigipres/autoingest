import os
import json
from ..resources import utils
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

# from media_pipeline.jobs.single_file_ingest import single_file_ingest_job ?



@sensor(
    job=validation_file_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def validation_folder_sensor(context: SensorEvaluationContext):
    # WATCH_FOLDER_PATHS to provide all paths at ingest/ level
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
            context.log.warning(f"Validation watch folder does not exist: {watch_path}")
            continue
        folders = [x for x in os.listdir(watch_dir) if os.path.isdir(os.path.join(watch_dir, x))]
        for folder in folders:
            fpath = os.path.join(watch_dir, folder)
            for file in os.listdir(fpath):
                file_path = os.path.join(fpath, file)
                if not file_path.is_file():
                    continue

                file_key = str(file_path)
                current_files.add(file_key)
                if file_key not in seen_files:
                    new_files.append(file_key)

    val_requests = []
    for file_key in new_files:
        val_requests.append(
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

    # Update cursor: keep current files, add new ones, prune gone files
    updated_seen = list(current_files | set(new_files))
    context.update_cursor(json.dumps(updated_seen))

    return val_requests
