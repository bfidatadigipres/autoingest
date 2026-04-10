from dagster import job
from dagster_celery import celery_executor

from autoingest.ops.archive.tape_transfer import batch_transfer_to_tape
from autoingest.ops.archive.verification import verify_tape_copies


@job(
    resource_defs={
        "workflow_db": "workflow_db",
        "spectralogic": "spectralogic",
    },
)
def tape_batch_archive_job():
    batch_result = batch_transfer_to_tape()
    verify_tape_copies(batch_result)
