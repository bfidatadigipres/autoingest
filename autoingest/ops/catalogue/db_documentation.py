from ...resources import utils
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
            f"Creating catalogue record for {file_info['file_name']}"
        )
    else:
        context.log.warning(f"File {file_info['file_name']} does not have file_status 'File cleared for ingest'")
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
        "mdata_full_json",
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
        "source",
        "put_type",
        "autoingest_path"
    ]

    basics = {}
    for meta in basic_metadata:
        if file_info.get(meta):
            basics[meta] = file_info.get(meta)
        else:
            context.log.warning(f"Missing metadata {meta} for file {file_info['file_name']}")

    try:
        db = context.resources.workflow_db
        record_id = db.create_file_record(basics)
        context.log.info(f"Catalogue record created with ID: {record_id}")
    except Exception as exc:
        context.log.warning(f"Failed to create record with data:\n{basics} {exc}")
        return None

    # Move file to PUT folders - conditional splits
    put_base = file_info.get("autoingest_path")
    source = Path(file_info["file_path"])
    base_dir = Path(source).parent.parent.parent
    autoingest_path = base_dir / put_base / file_info["file_name"]
    context.log.info(f"Moving {file_info['file_name']} to PUT folder: {autoingest_path}")
    
    db.update_file_status(record_id, file_status="File cleared for ingest")
    try:
        success, log = utils.move_file(source, autoingest_path)
    except Exception as exc:
        context.log.warning(f"Move error for {file_info['file_name']}:\n{exc}")
        db.update_file_status(record_id, file_status="Error")
        db.update_file_status(record_id, error_message="File failed move into autoingest processing folder")
        return None
    if success is True:
        context.log.info(log)
        db.update_file_status(record_id, file_status="File cleared for ingest")
        return record_id
    else:
        context.log.warning(f"Move failed for {file_info['file_name']}: {log}")
        db.update_file_status(record_id, file_status="Error")
        db.update_file_status(record_id, error_message="File failed move into autoingest processing folder")
        return None
