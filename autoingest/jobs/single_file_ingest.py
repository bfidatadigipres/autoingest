from dagster import job, In
from dagster_celery import celery_executor

from autoingest.ops.ingest.file_assessment import assess_filename
from autoingest.ops.ingest.metadata_extraction import extract_metadata, generate_checksum
from autoingest.ops.catalogue.db_documentation import create_catalogue_record


@job(
    resource_defs={
        "workflow_db": "workflow_db",
    },
)
def single_file_ingest_job():
    file_info = assess_filename()
    enriched = extract_metadata(file_info)
    checksummed = generate_checksum(enriched)
    create_catalogue_record(checksummed)
