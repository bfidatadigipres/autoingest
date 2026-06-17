import os
import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.validation_jobs import verify_local_job

RETRY_INTERVAL_SECONDS = 300


@sensor(
    job=verify_local_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def validation_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    validate_paths = os.environ.get("VALIDATION_FOLDER_PATHS", "").split(",")
    validate_paths = [p.strip() for p in validate_paths if p.strip()]

    if not validate_paths:
        context.log.warning("VALIDATION_FOLDER_PATHS is empty — no folders to watch.")
        return []

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

    context.log.info(
        f"Sensor tick — {len(validate_paths)} validation folder(s), "
        f"{len(cursor)} files in cursor"
    )

    db = context.resources.workflow_db
    now = int(time.time())

    new_requests = []
    current_files: dict[str, str] = {}  # file_path → file_name
    total_subdirs = 0
    skipped_ingest = 0
    skipped_not_dir = 0
    skipped_not_file = 0
    skipped_not_ready = 0

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
                current_files[file_key] = file_path.name

    # Prune cursor entries for files no longer on disk
    stale = {fp for fp in cursor if fp not in current_files}
    for fp in stale:
        del cursor[fp]

    for file_key, file_name in current_files.items():
        last_trigger = cursor.get(file_key)
        if last_trigger is not None and (now - last_trigger) < RETRY_INTERVAL_SECONDS:
            continue

        # Check DB for current file_status
        file_status = None
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_status FROM app.file_catalogue "
                    "WHERE file_name = %s ORDER BY created_at DESC LIMIT 1",
                    (file_name,),
                )
                row = cur.fetchone()
                if row:
                    file_status = row[0]

        # Only trigger if ready, or if stuck at 'validating' for re-trigger
        if file_status == "File cleared for ingest":
            pass
        elif file_status == "validating" and last_trigger is not None:
            # Stuck in validating from a crashed run — allow retrigger
            context.log.info(
                f"Retrying {file_name} — stuck at 'validating' for "
                f"{now - last_trigger}s (possible crash recovery)"
            )
        elif file_status is None:
            skipped_not_ready += 1
            context.log.info(
                f"Skipping {file_name} — no DB record found"
            )
            continue
        else:
            skipped_not_ready += 1
            context.log.info(
                f"Skipping {file_name} — status is '{file_status}', "
                f"expected 'File cleared for ingest'"
            )
            continue

        context.log.info(
            f"Validation sensor: launching verify for {file_key}"
            + (f" (retry, {now - last_trigger}s since last attempt)" if last_trigger else "")
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

    context.log.info(
        f"Scan complete — subdirs scanned={total_subdirs}, "
        f"skipped: not-dir={skipped_not_dir} ingest={skipped_ingest} "
        f"not-file={skipped_not_file} not-ready={skipped_not_ready}, "
        f"files found={len(current_files)}, new={len(new_requests)}"
    )

    if new_requests:
        context.log.info(f"Validation sensor: launching {len(new_requests)} run(s)")

    context.update_cursor(json.dumps(cursor))
    return new_requests
