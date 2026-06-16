import os
import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.validation_job import validation_job


@sensor(
    job=validation_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def validation_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    validate_paths = os.environ.get("VALIDATION_FOLDER_PATHS", "").split(",")
    validate_paths = [p.strip() for p in validate_paths if p.strip()]

    if not validate_paths:
        context.log.warning("VALIDATION_FOLDER_PATHS is empty — no folders to watch.")
        return []

    seen_files = set()
    if context.cursor:
        try:
            seen_files = set(json.loads(context.cursor))
        except (json.JSONDecodeError, TypeError):
            seen_files = set()

    context.log.info(
        f"Sensor tick — {len(validate_paths)} validation folder(s), "
        f"{len(seen_files)} files in cursor"
    )

    new_files = []
    current_files = set()
    total_subdirs = 0
    skipped_ingest = 0
    skipped_not_dir = 0
    skipped_not_file = 0

    for validate_path in validate_paths:
        validate_dir = Path(validate_path)
        if not validate_dir.exists():
            context.log.warning(f"Validation folder does not exist: {validate_path}")
            continue

        for subfolder in validate_dir.iterdir():
            total_subdirs += 1

            if not subfolder.is_dir():
                skipped_not_dir += 1
                continue
            if subfolder.name.startswith("ingest_"):
                skipped_ingest += 1
                context.log.info(f"Skipping incomplete ingest folder: {subfolder.name}")
                continue

            for file_path in subfolder.iterdir():
                if not file_path.is_file():
                    skipped_not_file += 1
                    continue

                file_key = str(file_path)
                current_files.add(file_key)

                if file_key not in seen_files:
                    context.log.info(
                        f"New file detected in {subfolder.name}: "
                        f"{file_path.name}"
                    )
                    new_files.append(file_key)

    context.log.info(
        f"Scan complete — subdirs scanned={total_subdirs}, "
        f"skipped: not-dir={skipped_not_dir} ingest={skipped_ingest} not-file={skipped_not_file}, "
        f"files found={len(current_files)}, new={len(new_files)}"
    )

    val_requests = []
    for file_key in new_files:
        val_requests.append(
            RunRequest(
                run_key=f"validate-{file_key.name}-{int(time.time())}",
                run_config={
                    "ops": {
                        "verify_tape_copy": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )

    updated_seen = list(current_files)
    context.update_cursor(json.dumps(updated_seen))
    context.log.info(f"Cursor updated: {len(updated_seen)} files tracked")
    return val_requests
