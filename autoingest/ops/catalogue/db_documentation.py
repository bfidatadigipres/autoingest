from dagster import op, Out


@op(
    out={"file_id": Out(int)},
    tags={"dagster-celery/queue": "default"},
)
def create_catalogue_record(context, checksummed_file_info: dict, workflow_db) -> int:
    context.log.info(
        f"Creating catalogue record for {checksummed_file_info['filename']}"
    )

    basics = {
        "file_name": checksummed_file_info.get("file_name"),
        "file_path": checksummed_file_info.get("file_path"),
        "extension": checksummed_file_info.get("extension"),
        "file_size": checksummed_file_info.get("file_size"),
        "status": "Cleared for ingest"
    }

    updates = checksummed_file_info
    updates.pop("file_name", None)
    updates.pop("file_path", None)
    updates.pop("extension", None)
    updates.pop("file_size", None)

    file_id = workflow_db.create_file_record(basics)
    context.log.info(f"Catalogue record created with ID: {file_id}")

    fields = workflow_db.update_file_status(file_id, updates)
    context.log.info(f"Updating remaining fields to database:\n{fields}")

    return file_id