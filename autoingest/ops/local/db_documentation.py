from ...resources import utils
import time
from pathlib import Path
from dagster import op, Output, OpExecutionContext


@op(required_resource_keys={"workflow_db"}, config_schema={"file_path": str})
def create_catalogue_record(context: OpExecutionContext) -> Output:
    tic = time.perf_counter()
    file_path = Path(context.op_config["file_path"])
    file_name = file_path.name
    db = context.resources.workflow_db

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, file_name, file_path, extension, file_size,
                       error_message, incomplete_scan, screencraft_arch,
                       checksum_md5, checksum_date, checksum_xxh,
                       mdata_full_text, mdata_text, mdata_ebucore,
                       mdata_pbcore, mdata_full_xml, mdata_full_json::text,
                       whole, part, do_ingest, ffprobe_exit,
                       bp_bucket, bucket_list, mime_type,
                       cid_file_type, cid_item_priref, cid_ob_num,
                       source, put_type, autoingest_path,
                       file_status
                FROM app.file_catalogue
                WHERE file_name = %s
                ORDER BY created_at DESC LIMIT 1
            """, (file_name,))
            row = cur.fetchone()

    if not row:
        context.log.warning(f"No DB record found for {file_name}")
        return Output(None, metadata={"duration_sec": round(time.perf_counter() - tic, 3), "preview": f"No record: {file_name}"})

    cols = [
        "id", "file_name", "file_path", "extension", "file_size",
        "error_message", "incomplete_scan", "screencraft_arch",
        "checksum_md5", "checksum_date", "checksum_xxh",
        "mdata_full_text", "mdata_text", "mdata_ebucore",
        "mdata_pbcore", "mdata_full_xml", "mdata_full_json",
        "whole", "part", "do_ingest", "ffprobe_exit",
        "bp_bucket", "bucket_list", "mime_type",
        "cid_file_type", "cid_item_priref", "cid_ob_num",
        "source", "put_type", "autoingest_path",
        "file_status",
    ]
    record = dict(zip(cols, row))
    file_status = record.get("file_status", "")
    record_id = record["id"]

    if file_status == "cataloguing":
        context.log.info(f"File {file_name} is already being catalogued. Skipping.")
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"Already cataloguing: {file_name}",
        })
    if file_status != "checksummed":
        context.log.warning(
            f"File {file_name} has status '{file_status}' — expected 'checksummed'. Skipping."
        )
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"Skipped: status={file_status}",
        })

    _set_cataloguing_status(db, record_id)

    context.log.info(f"Creating catalogue record for {file_name} (status: {file_status})")

    basic_metadata = [
        "file_name", "file_path", "extension", "file_size", "error_message",
        "incomplete_scan", "screencraft_arch", "checksum_md5", "checksum_date",
        "checksum_xxh", "mdata_full_text", "mdata_text", "mdata_ebucore",
        "mdata_pbcore", "mdata_full_xml", "mdata_full_json", "whole", "part",
        "do_ingest", "ffprobe_exit", "bp_bucket", "bucket_list", "mime_type",
        "cid_file_type", "cid_item_priref", "cid_ob_num", "source",
        "put_type", "autoingest_path"
    ]

    basics = {}
    for meta in basic_metadata:
        val = record.get(meta)
        if val is not None:
            basics[meta] = val
        else:
            context.log.warning(f"Missing metadata {meta} for file {file_name}")

    db_insert_tic = time.perf_counter()
    try:
        rid, action = db.upsert_file_record(basics)
        context.log.info(f"Catalogue record {action}: ID {rid} for {file_name}")
    except Exception as exc:
        context.log.warning(f"Failed to create record for {file_name}: {exc}")
        _rollback_cataloguing_status(db, record_id)
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"DB upsert failed: {file_name}"})
    db_insert_toc = time.perf_counter()
    db_insert_time = round(db_insert_toc - db_insert_tic, 3)

    if action == "skip":
        context.log.info(f"Skipping file move for {file_name} — already in progress")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(record_id, metadata={
            "duration_sec": duration_sec,
            "record_id": record_id,
            "preview": f"Skipped (already processing): {file_name}",
        })

    put_base = record.get("autoingest_path")
    if not put_base:
        context.log.warning(f"Missing autoingest_path for {file_name} — cannot move file.")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"Missing autoingest_path: {file_name}"})

    source = Path(record["file_path"])
    base_dir = source.parent.parent.parent.parent
    autoingest_path = base_dir / put_base / file_name
    context.log.info(f"Moving {file_name} to PUT folder: {autoingest_path}")

    move_tic = time.perf_counter()
    try:
        success, log = utils.move_file(source, autoingest_path)
    except Exception as exc:
        context.log.warning(f"Move error for {file_name}:\n{exc}")
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.file_catalogue SET file_status = 'Error', "
                    "error_message = %s, updated_at = NOW() WHERE id = %s",
                    ("File failed move into autoingest processing folder", record_id),
                )
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"Move failed: {file_name}"})
    move_toc = time.perf_counter()
    move_time = round(move_toc - move_tic, 3)

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)

    record_id_out = record_id if success is True else None
    if success is True:
        context.log.info(log)
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    db._retry_query(conn, cur, """
                        UPDATE app.file_catalogue
                        SET file_status = 'File cleared for ingest',
                            do_ingest = %s,
                            error_message = %s,
                            file_path = %s,
                            extension = %s,
                            file_size = %s,
                            incomplete_scan = %s,
                            screencraft_arch = %s,
                            checksum_md5 = %s,
                            checksum_xxh = %s,
                            checksum_date = %s,
                            part = %s,
                            whole = %s,
                            ffprobe_exit = %s,
                            mime_type = %s,
                            bp_bucket = %s,
                            bucket_list = %s,
                            cid_file_type = %s,
                            cid_item_priref = %s,
                            cid_ob_num = %s,
                            source = %s,
                            put_type = %s,
                            autoingest_path = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (
                        basics.get("do_ingest", ""),
                        basics.get("error_message", ""),
                        str(basics.get("file_path", "")),
                        basics.get("extension", ""),
                        basics.get("file_size", 0),
                        basics.get("incomplete_scan", ""),
                        basics.get("screencraft_arch", ""),
                        basics.get("checksum_md5", ""),
                        basics.get("checksum_xxh", ""),
                        basics.get("checksum_date", ""),
                        basics.get("part"),
                        basics.get("whole"),
                        basics.get("ffprobe_exit"),
                        basics.get("mime_type", ""),
                        basics.get("bp_bucket", ""),
                        str(basics.get("bucket_list", "")),
                        basics.get("cid_file_type", ""),
                        basics.get("cid_item_priref", ""),
                        basics.get("cid_ob_num", ""),
                        basics.get("source", ""),
                        basics.get("put_type", ""),
                        basics.get("autoingest_path", ""),
                        record_id,
                    ), context.log)
        except Exception as exc:
            _rollback_cataloguing_status(db, record_id)
            context.log.error(f"Failed to write 'File cleared for ingest' status for {file_name}: {exc}")
            duration_sec = round(time.perf_counter() - tic, 3)
            return Output(None, metadata={"duration_sec": duration_sec, "preview": f"DB write failed: {file_name}"})
    else:
        context.log.warning(f"Move failed for {file_name}: {log}")
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    db._retry_query(conn, cur,
                        "UPDATE app.file_catalogue SET file_status = 'Error', "
                        "error_message = %s, updated_at = NOW() WHERE id = %s",
                        ("File failed move into autoingest processing folder", record_id),
                        context.log,
                    )
        except Exception as exc:
            context.log.error(f"Failed to write Error status for {file_name}: {exc}")

    metadata = {
        "duration_sec": duration_sec,
        "file_name": file_name,
        "record_id": record_id,
        "db_insert_time_sec": db_insert_time,
        "move_time_sec": move_time,
        "action": action,
        "preview": f"{file_name} catalogued ({action}) in {duration_sec}s (DB: {db_insert_time}s, move: {move_time}s)",
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
    except Exception as exc:
        context.log.warning(f"Failed to record pipeline event for {file_name}: {exc}")

    return Output(record_id_out, metadata=metadata)


def _set_cataloguing_status(db, record_id: int) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app.file_catalogue SET file_status = 'cataloguing', "
                "error_message = NULL, updated_at = NOW() WHERE id = %s",
                (record_id,),
            )


def _rollback_cataloguing_status(db, record_id: int) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app.file_catalogue SET file_status = 'checksummed', "
                "updated_at = NOW() WHERE id = %s",
                (record_id,),
            )
