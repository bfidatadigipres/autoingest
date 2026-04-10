from dagster import Definitions, EnvVar
from dagster_celery import celery_executor

from autoingest.resources.database import workflow_database
from autoingest.resources.spectralogic import spectralogic_client
from autoingest.resources.encoding import encoding_config

from autoingest.jobs.single_file_ingest import single_file_ingest_job
from autoingest.jobs.tape_batch_archive import tape_batch_archive_job
from autoingest.jobs.encoding_job import encoding_job
from autoingest.jobs.cleanup_job import cleanup_job

from autoingest.sensors.watch_folder import watch_folder_sensor
from autoingest.schedules.batch_schedule import tape_batch_schedule


celery_executor_configured = celery_executor.configured(
    {
        "broker": {"env": "CELERY_BROKER_URL"},
        "backend": {"env": "CELERY_RESULT_BACKEND"},
        "config_source": {"task_always_eager": False},
    },
    name="celery_redis",
)


defs = Definitions(
    jobs=[
        single_file_ingest_job,
        tape_batch_archive_job,
        encoding_job,
        cleanup_job,
    ],
    sensors=[
        watch_folder_sensor,
    ],
    schedules=[
        tape_batch_schedule,
    ],
    resources={
        "workflow_db": workflow_database,
        "spectralogic": spectralogic_client,
        "encoding_config": encoding_config,
    },
    executors=[celery_executor_configured],
)
