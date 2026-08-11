from dagster import Definitions

from autoingest.resources.database import workflow_database
from autoingest.resources.encoding import encoding_config
from autoingest.resources.io_manager import postgres_io_manager

from autoingest.jobs.ingest_jobs import (
    ingest_local_job,
    ingest_celery_job,
    catalogue_local_job,
)
from autoingest.jobs.validation_jobs import (
    verify_local_job,
    encoding_celery_job,
    generate_images_job,
    cleanup_local_job,
    metadata_update_local_job,
)
from autoingest.jobs.cleanup_job import cleanup_job, cleanup_schedule

from autoingest.sensors.watch_folder import watch_folder_sensor
from autoingest.sensors.validation_folder import validation_folder_sensor
from autoingest.sensors.chain_sensors import (
    ingest_chain_sensor,
    catalogue_chain_sensor,
    encoding_chain_sensor,
    generate_images_chain_sensor,
    cleanup_status_sensor,
    metadata_update_chain_sensor,
)


defs = Definitions(
    jobs=[
        # Ingest pipeline — 3 chained jobs
        ingest_local_job,
        ingest_celery_job,
        catalogue_local_job,
        # Validation pipeline — 4 chained jobs
        verify_local_job,
        encoding_celery_job,
        generate_images_job,
        cleanup_local_job,
        metadata_update_local_job,
        # Standalone cleanup sweep
        cleanup_job,
    ],
    sensors=[
        # Folder scanners
        watch_folder_sensor,
        validation_folder_sensor,
        # Pipeline chainers (DB status-driven)
        ingest_chain_sensor,
        catalogue_chain_sensor,
        encoding_chain_sensor,
        generate_images_chain_sensor,
        cleanup_status_sensor,
        metadata_update_chain_sensor,
    ],
    schedules=[cleanup_schedule],
    resources={
        "workflow_db": workflow_database,
        "encoding_config": encoding_config,
        "io_manager": postgres_io_manager,
    },
)
