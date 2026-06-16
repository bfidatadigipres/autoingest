import time
from pathlib import Path
from dagster import run_status_sensor, RunStatusSensorContext, DagsterRunStatus, RunRequest

from autoingest.jobs.ingest_jobs import (
    ingest_local_job,
    ingest_celery_job,
    catalogue_local_job,
)
from autoingest.jobs.validation_jobs import (
    verify_local_job,
    encoding_celery_job,
    cleanup_local_job,
)


# ── Ingest pipeline chain ──────────────────────────────────────

@run_status_sensor(
    monitored_jobs=[ingest_local_job],
    run_status=DagsterRunStatus.SUCCESS,
    request_job=ingest_celery_job,
)
def on_ingest_local_success(context: RunStatusSensorContext) -> RunRequest:
    run_config = context.dagster_run.run_config or {}
    file_path = run_config.get("ops", {}).get("assess_filename", {}).get("config", {}).get("file_path", "")
    return RunRequest(
        run_key=f"checksum-{Path(file_path).name}-{int(time.time())}",
        run_config={
            "ops": {
                "generate_checksum": {
                    "config": {"file_path": file_path}
                }
            }
        },
    )


@run_status_sensor(
    monitored_jobs=[ingest_celery_job],
    run_status=DagsterRunStatus.SUCCESS,
    request_job=catalogue_local_job,
)
def on_ingest_celery_success(context: RunStatusSensorContext) -> RunRequest:
    run_config = context.dagster_run.run_config or {}
    file_path = run_config.get("ops", {}).get("generate_checksum", {}).get("config", {}).get("file_path", "")
    return RunRequest(
        run_key=f"catalogue-{Path(file_path).name}-{int(time.time())}",
        run_config={
            "ops": {
                "create_catalogue_record": {
                    "config": {"file_path": file_path}
                }
            }
        },
    )


# ── Validation pipeline chain ──────────────────────────────────

@run_status_sensor(
    monitored_jobs=[verify_local_job],
    run_status=DagsterRunStatus.SUCCESS,
    request_job=encoding_celery_job,
)
def on_verify_local_success(context: RunStatusSensorContext) -> RunRequest:
    run_config = context.dagster_run.run_config or {}
    file_path = run_config.get("ops", {}).get("verify_tape_copy", {}).get("config", {}).get("file_path", "")
    return RunRequest(
        run_key=f"encode-{Path(file_path).name}-{int(time.time())}",
        run_config={
            "ops": {
                "encode_proxy_mp4": {
                    "config": {"file_path": file_path}
                }
            }
        },
    )


@run_status_sensor(
    monitored_jobs=[encoding_celery_job],
    run_status=DagsterRunStatus.SUCCESS,
    request_job=cleanup_local_job,
)
def on_encoding_celery_success(context: RunStatusSensorContext) -> RunRequest:
    run_config = context.dagster_run.run_config or {}
    file_path = run_config.get("ops", {}).get("encode_proxy_mp4", {}).get("config", {}).get("file_path", "")
    return RunRequest(
        run_key=f"cleanup-{Path(file_path).name}-{int(time.time())}",
        run_config={
            "ops": {
                "check_and_delete_source": {
                    "config": {"file_path": file_path}
                }
            }
        },
    )
