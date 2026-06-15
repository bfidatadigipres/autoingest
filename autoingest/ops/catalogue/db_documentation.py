from typing import Optional
from ...resources import utils
import time
from pathlib import Path
from dagster import op, Output


@op(required_resource_keys={"workflow_db"})
def create_catalogue_record(context, file_info: dict) -> Output:
    """
    Verify ingest of file accepted and write all metadata
    to new database record. Move file to PUT folder path. 
    """
    tic = time.perf_counter()
    file_name = file_info.get("file_name", "unknown")

    if file_info.get("file_status") == "File cleared for ingest":
        context.log.info(
            f"Creating catalogue record for {file_info['file_name']}"
        )
    else:
        context.log.warning(f"File {file_info['file_name']} does not have file_status 'File cleared for ingest'")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"Skipped: {file_name}"})

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

    db_insert_tic = time.perf_counter()
    try:
        db = context.resources.workflow_db
        record_id = db.create_file_record(basics)
        context.log.info(f"Catalogue record created with ID: {record_id}")
    except Exception as exc:
        context.log.warning(f"Failed to create record with data:\n{basics} {exc}")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"DB insert failed: {file_name}"})
    db_insert_toc = time.perf_counter()
    db_insert_time = round(db_insert_toc - db_insert_tic, 3)

    # Move file to PUT folders - conditional splits
    put_base = file_info.get("autoingest_path")
    source = Path(file_info["file_path"])
    base_dir = Path(source).parent.parent.parent
    autoingest_path = base_dir / put_base / file_info["file_name"]
    context.log.info(f"Moving {file_info['file_name']} to PUT folder: {autoingest_path}")
    
    db.update_file_status(record_id, file_status="File cleared for ingest")
    move_tic = time.perf_counter()
    try:
        success, log = utils.move_file(source, autoingest_path)
    except Exception as exc:
        context.log.warning(f"Move error for {file_info['file_name']}:\n{exc}")
        db.update_file_status(record_id, file_status="Error")
        db.update_file_status(record_id, error_message="File failed move into autoingest processing folder")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"Move failed: {file_name}"})
    move_toc = time.perf_counter()
    move_time = round(move_toc - move_tic, 3)

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)

    record_id_out = record_id if success is True else None
    if success is True:
        context.log.info(log)
        db.update_file_status(record_id, file_status="File cleared for ingest")
    else:
        context.log.warning(f"Move failed for {file_info['file_name']}: {log}")
        db.update_file_status(record_id, file_status="Error")
        db.update_file_status(record_id, error_message="File failed move into autoingest processing folder")

    metadata = {
        "duration_sec": duration_sec,
        "file_name": file_name,
        "record_id": record_id,
        "db_insert_time_sec": db_insert_time,
        "move_time_sec": move_time,
        "preview": f"{file_name} catalogued in {duration_sec}s (DB: {db_insert_time}s, move: {move_time}s)",
    }

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="create_catalogue_record",
            event_type="op_completed",
            status="success" if record_id_out is not None else "failure",
            metadata=metadata,
        )
    except Exception:
        pass

    return Output(record_id_out, metadata=metadata)
