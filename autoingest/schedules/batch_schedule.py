from dagster import ScheduleDefinition
from autoingest.jobs.tape_batch_archive import tape_batch_archive_job


# Run tape batching every 30 minutes
tape_batch_schedule = ScheduleDefinition(
    job=tape_batch_archive_job,
    cron_schedule="*/30 * * * *",
    execution_timezone="Europe/London",
)
