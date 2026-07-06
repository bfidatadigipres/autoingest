import time
from pathlib import Path
import autoingest.resources.utils as utils
from datetime import datetime
from dagster import op, Out, Output, OpExecutionContext


@op(
    tags={"dagster-celery/queue": "checksum"},
    out=Out(dict),
    required_resource_keys={"workflow_db"},
    config_schema={"file_path": str},
)
def generate_checksum(context: OpExecutionContext) -> Output:
    tic = time.perf_counter()

    file_path = context.op_config["file_path"]
    file_name = Path(file_path).name

    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, file_status, do_ingest, file_size, file_path "
                "FROM app.file_catalogue WHERE file_name = %s "
                "ORDER BY created_at DESC LIMIT 1",
                (file_name,),
            )
            row = cur.fetchone()

    if not row:
        context.log.info(f"No DB record found for {file_name}. Skipping.")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

    file_id, file_status, do_ingest, file_size, stored_path = row

    if file_status == "generating_checksum":
        context.log.info(f"File {file_name} is already being checksummed. Skipping.")
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"Already checksumming: {file_name}",
        })

    if do_ingest != "TRUE":
        context.log.info(f"Skipping checksum generation — file not cleared for ingest: {file_name}")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

    context.log.info(f"Generating MD5 checksum for {file_name}")
    _set_encoding_status(db, file_id)
    md5 = xxhash_val = None
    try:
        md5_tic = time.perf_counter()
        md5 = utils.create_md5_65536(file_path)
        md5_toc = time.perf_counter()

        context.log.info(f"Generating XXHash checksum for {file_name}")
        xxh_tic = time.perf_counter()
        xxhash_val = utils.create_xxhash_65536(file_path)
        xxh_toc = time.perf_counter()
    except Exception:
        _rollback_checksum_status(db, file_id)
        context.log.error(f"Checksum generation failed for {file_name}")
        raise

    checksum_date = str(datetime.now())[:10]
    md5_time = round(md5_toc - md5_tic, 3)
    xxh_time = round(xxh_toc - xxh_tic, 3)

    context.log.info(f"Checksum MD5: {md5} / Checksum XXHash: {xxhash_val}")

    checksum_duration = round(time.perf_counter() - tic, 3)

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app.file_catalogue
                SET checksum_md5 = %s,
                    checksum_xxh = %s,
                    checksum_date = %s,
                    checksum_time_sec = %s,
                    file_status = 'checksummed',
                    updated_at = NOW()
                WHERE id = %s
            """, (md5, xxhash_val, checksum_date, checksum_duration, file_id))

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)

    result = {
        "file_id": file_id,
        "file_path": file_path,
        "file_name": file_name,
        "file_size": file_size,
        "checksum_md5": md5,
        "checksum_xxh": xxhash_val,
        "checksum_date": checksum_date,
    }

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="generate_checksum",
            event_type="op_completed",
            status="success" if md5 and xxhash_val else "failure",
            metadata={
                "duration_sec": duration_sec,
                "file_name": file_name,
                "file_size": file_size,
                "md5_time_sec": md5_time,
                "xxhash_time_sec": xxh_time,
                "md5_throughput_mbps": round(file_size / md5_time / 1048576, 2) if md5_time > 0 else 0,
                "xxh_throughput_mbps": round(file_size / xxh_time / 1048576, 2) if xxh_time > 0 else 0,
                "checksum_md5": md5[:8] if md5 else None,
                "preview": f"{file_name} checksums in {duration_sec}s (MD5: {md5_time}s, XXH: {xxh_time}s)",
            },
        )
    except Exception:
        pass

    return Output(result, metadata={
        "duration_sec": duration_sec,
        "file_name": file_name,
        "file_size": file_size,
        "md5_time_sec": md5_time,
        "xxhash_time_sec": xxh_time,
        "preview": f"{file_name} checksums in {duration_sec}s",
    })


def _set_encoding_status(db, file_id: int) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app.file_catalogue SET file_status = 'generating_checksum', "
                "error_message = NULL, updated_at = NOW() WHERE id = %s",
                (file_id,),
            )


def _rollback_checksum_status(db, file_id: int) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app.file_catalogue SET file_status = 'assessed', "
                "updated_at = NOW() WHERE id = %s",
                (file_id,),
            )
