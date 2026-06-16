from dagster import job
from dagster_celery import celery_executor

from autoingest.ops.ingest.file_assessment import assess_filename
from autoingest.ops.ingest.metadata_extraction import extract_metadata, generate_checksum
from autoingest.ops.catalogue.db_documentation import create_catalogue_record

celery_exec = celery_executor.configured(
    {
        "broker": {"env": "CELERY_BROKER_URL"},
        "backend": {"env": "CELERY_RESULT_BACKEND"},
        "config_source": {"task_always_eager": False},
    },
    name="celery_redis",
)


@job(executor_def=celery_exec)
def single_file_ingest_job():
    file_info = assess_filename()
    enriched = extract_metadata(file_info)
    checksummed = generate_checksum(enriched)
    create_catalogue_record(checksummed)
