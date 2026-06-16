from dagster import graph

from autoingest.ops.local.file_assessment import assess_filename
from autoingest.ops.local.extract_metadata import extract_metadata
from autoingest.ops.local.db_documentation import create_catalogue_record
from autoingest.ops.celery.checksum import generate_checksum


@graph
def ingest_local_graph():
    file_info = assess_filename()
    extract_metadata(file_info)


@graph
def ingest_celery_graph():
    return generate_checksum()


@graph
def catalogue_graph():
    return create_catalogue_record()
