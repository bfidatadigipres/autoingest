import os
import json
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.validation_job import validation_job


@sensor(
    job=validation_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def validation_folder_sensor(context: SensorEvaluationContext):
    validate_paths = os.environ.get("VALIDATION_FOLDER_PATHS", "").split(",")
    validate_paths = [p.strip() for p in validate_paths if p.strip()]

    seen_files = set()
    if context.cursor:
        try:
            seen_files = set(json.loads(context.cursor))
        except (json.JSONDecodeError, TypeError):
            seen_files = set()

    new_files = []
    current_files = set()

    for validate_path in validate_paths:
        validate_dir = Path(validate_path)
        if not validate_dir.exists():
            context.log.warning(f"Validation folder does not exist: {validate_path}")
            continue

        for subfolder in validate_dir.iterdir():
            if not subfolder.is_dir():
                continue
            if subfolder.startswith("ingest_"):
                context.log.info(f"Skipping incomplete folder: {subfolder} for path {validate_dir}")
                continue
            for file_path in subfolder.iterdir():
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
                run_key=f"validate-{file_key}",
                run_config={
                    "ops": {
                        "verify_tape_copy": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )

    updated_seen = list(current_files | set(new_files))
    context.update_cursor(json.dumps(updated_seen))
    return val_requests