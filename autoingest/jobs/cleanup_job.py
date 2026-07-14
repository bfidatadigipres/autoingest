from autoingest.graphs.cleanup_graph import sweep_graph
from dagster import ScheduleDefinition

cleanup_job = sweep_graph.to_job(
    name="cleanup_job",
    description="Sweep completed files and clean up source. Runs in-process on DATA15.",
)

cleanup_schedule = ScheduleDefinition(
    job=cleanup_job,
    cron_schedule="0 0 1 1,7 *",
)
