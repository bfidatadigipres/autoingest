from dagster import Definitions

from autoingest.resources.database import workflow_database
from autoingest.resources.encoding import encoding_config

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
from autoingest.jobs.cleanup_job import cleanup_job

from autoingest.sensors.watch_folder import watch_folder_sensor
from autoingest.sensors.validation_folder import validation_folder_sensor
from autoingest.sensors.chain_sensors import (
    on_ingest_local_success,
    on_ingest_celery_success,
    on_verify_local_success,
    on_encoding_celery_success,
)


defs = Definitions(
    jobs=[
        # Ingest pipeline — 3 chained jobs
        ingest_local_job,
        ingest_celery_job,
        catalogue_local_job,
        # Validation pipeline — 3 chained jobs
        verify_local_job,
        encoding_celery_job,
        cleanup_local_job,
        # Standalone cleanup sweep
        cleanup_job,
    ],
    sensors=[
        # Folder scanners
        watch_folder_sensor,
        validation_folder_sensor,
        # Pipeline chainers
        on_ingest_local_success,
        on_ingest_celery_success,
        on_verify_local_success,
        on_encoding_celery_success,
    ],
    resources={
        "workflow_db": workflow_database,
        "encoding_config": encoding_config,
    },
)
