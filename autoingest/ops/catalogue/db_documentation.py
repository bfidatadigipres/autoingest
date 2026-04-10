from dagster import op, Out


@op(
    out={"file_id": Out(int)},
    tags={"dagster-celery/queue": "default"},
)
def create_catalogue_record(context, checksummed_file_info: dict, workflow_db) -> int:
    context.log.info(
        f"Creating catalogue record for {checksummed_file_info['filename']}"
    )

    file_data = {
        "filename": checksummed_file_info["filename"],
        "filepath": checksummed_file_info["file_path"],
        "filetype": checksummed_file_info["filetype"],
        "filesize": checksummed_file_info["filesize"],
        "checksum_md5": checksummed_file_info["checksum_md5"],
        "metadata_json": checksummed_file_info.get("metadata_json", "{}"),
        "tape_object_id": None,
        "tape_verified": False,
        "proxy_created": False,
        "status": "metadata_complete",
    }

    file_id = workflow_db.create_file_record(file_data)
    context.log.info(f"Catalogue record created with ID: {file_id}")
    return file_id
