from dagster import op, OpExecutionContext


@op(required_resource_keys={"workflow_db"})
def create_catalogue_record(context: OpExecutionContext, file_info: dict) -> int:
    context.log.info(
        f"Creating catalogue record for {file_info['filename']}"
    )

    basics = {
        "file_name": file_info.get("file_name"),
        "file_path": file_info.get("file_path"),
        "extension": file_info.get("extension"),
        "file_size": file_info.get("file_size"),
        "status": "Cleared for ingest"
    }

    updates = file_info
    updates.pop("file_name", None)
    updates.pop("file_path", None)
    updates.pop("extension", None)
    updates.pop("file_size", None)

    db = context.resources.workflow_db
    file_id = db.create_file_record(basics)
    context.log.info(f"Catalogue record created with ID: {file_id}")

    fields = db.update_file_status(file_id, updates)
    context.log.info(f"Updating remaining fields to database:\n{fields}")

    return file_id