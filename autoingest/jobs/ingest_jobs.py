from dagster_celery import celery_executor

from autoingest.graphs.ingest_graphs import (
    ingest_local_graph,
    ingest_celery_graph,
    catalogue_graph,
)

celery_exec = celery_executor.configured(
    {
        "broker": {"env": "CELERY_BROKER_URL"},
        "backend": {"env": "CELERY_RESULT_BACKEND"},
        "config_source": {"task_always_eager": False},
    },
    name="celery_redis",
)

ingest_local_job = ingest_local_graph.to_job(
    name="ingest_local_job",
    description="Runs assess_filename + extract_metadata locally on DATA15. "
                "Sets file_status = 'assessed' on success.",
)

ingest_celery_job = ingest_celery_graph.to_job(
    name="ingest_celery_job",
    executor_def=celery_exec,
    description="Runs generate_checksum on Celery encoding workers. "
                "Sets file_status = 'checksummed' on success.",
)

catalogue_local_job = catalogue_graph.to_job(
    name="catalogue_local_job",
    description="Runs create_catalogue_record locally on DATA15. "
                "Reads metadata from DB, moves file, sets file_status = 'File cleared for ingest'.",
)
