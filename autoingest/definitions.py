from dagster import Definitions

from autoingest.resources.database import workflow_database
from autoingest.resources.encoding import encoding_config

from autoingest.jobs.single_file_ingest import single_file_ingest_job
from autoingest.jobs.validation_job import validate_files_job
from autoingest.jobs.cleanup_job import cleanup_job

from autoingest.sensors.watch_folder import watch_folder_sensor
from autoingest.sensors.validation_folder import validation_folder_sensor


defs = Definitions(
    jobs=[
        single_file_ingest_job,
        validate_files_job,
        cleanup_job,
    ],
    sensors=[
        watch_folder_sensor,
        validation_folder_sensor,
    ],
    resources={
        "workflow_db": workflow_database,
        "encoding_config": encoding_config,
    },
)