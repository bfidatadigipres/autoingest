import os
import shutil
from pathlib import Path
from dagster import op


@op(required_resource_keys={"workflow_db"})
def create_catalogue_record(context, file_info: dict) -> int:
    """
    Verify ingest of file accepted and write all metadata
    to new database record. Move file to PUT folder path. 
    """

    if file_info.get("file_status") == "File cleared for ingest":
        context.log.info(
            f"Creating catalogue record for {file_info['filename']}"
        )
    else:
        context.log.warning(f"File {file_info['filename']} does not have file_status 'File cleared for ingest'")
        return None

    basic_metadata = [
        "file_name",
        "file_path",
        "extension",
        "file_size",
        "error_message",
        "incomplete_scan",
        "screencraft_arch",
        "checksum_md5",
        "checksum_date",
        "checksum_xxh",
        "mdata_full_text",
        "mdata_text",
        "mdata_ebucore",
        "mdata_pbcore",
        "mdata_full_xml",
        "mdata_full_json"
        "whole",
        "part",
        "do_ingest",
        "ffprobe_exit",
        "bp_bucket",
        "bucket_list",
        "mime_type",
        "cid_file_type",
        "cid_item_priref",
        "cid_ob_num",
        "source"
    ]

    basics = {}
    for meta in basic_metadata:
        if file_info.get(meta):
            basics[meta] = file_info.get(meta)
        else:
            context.log.warning(f"Missing metadata {meta} for file {file_info["file_name"]}")

    try:
        db = context.resources.workflow_db
        record_id = db.create_file_record(basics)
        context.log.info(f"Catalogue record created with ID: {record_id}")
    except Exception as err:
        context.log.warning(f"Failed to create record with data:\n{basics}")
        return None

    # Move file to PUT folder
    put_base = file_info.get("autoingest_path")
    source = Path(file_info["file_path"])
    destination = Path(put_base) / file_info["file_name"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    db.update_file_status(record_id, file_status="File cleared for ingest")
    try:
        shutil.move(str(source), str(destination))
        context.log.info(f"Moved {file_info['file_name']} to PUT folder: {destination}")
        db.update_file_status(record_id, file_status="File cleared for ingest")
        return record_id
    except Exception as err:
        context.log.warning(f"Move error for {file_info['file_name']}:\n{err}")
        db.update_file_status(record_id, file_status="Error")
        db.update_file_status(record_id, error_message="File failed move into autoingest_path folder")
        return None
