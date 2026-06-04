from dagster import job

from autoingest.ops.ingest.file_assessment import assess_filename
from autoingest.ops.ingest.metadata_extraction import extract_metadata, generate_checksum
from autoingest.ops.catalogue.db_documentation import create_catalogue_record


@job
def single_file_ingest_job():
    file_info = assess_filename()
    enriched = extract_metadata(file_info)
    checksummed = generate_checksum(enriched)
    create_catalogue_record(checksummed)
