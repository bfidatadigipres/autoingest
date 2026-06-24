from hashlib import md5
import os
import time
import datetime
from pathlib import Path
from dagster import op, OpExecutionContext, Output
from ...resources import utils


@op(required_resource_keys={"workflow_db"}, config_schema={"file_path": str})
def check_and_delete_source(context: OpExecutionContext) -> Output:
    tic = time.perf_counter()
    file_path_str = context.op_config["file_path"]
    file_name = Path(file_path_str).name

    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, file_path, bp_job_id, proxy_video_path, proxy_image_path, "
                "proxy_thumb_path, cid_media_priref, checksum_md5, checksum_date "
                "FROM app.file_catalogue WHERE file_name = %s "
                "ORDER BY created_at DESC LIMIT 1",
                (file_name,),
            )
            row = cur.fetchone()

    if not row:
        context.log.error(f"File {file_name} not found in catalogue")
        return Output(None, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

    file_id = row[0]
    bp_job_id = row[2] or ""
    proxy_video_path = row[3] or ""
    proxy_image_path = row[4] or ""
    proxy_thumb_path = row[5] or ""
    media_priref = row[6] or ""
    checksum_md5 = row[7] or ""
    checksum_date = row[7] or ""

    root = Path(row[1]).parent.parent.parent.parent
    source_path = root / "autoingest" / "validate" / bp_job_id / file_name

    media_data = []
    if proxy_video_path:
        media_data.append(f"<access_rendition.mp4>{Path(proxy_video_path).name}</access_rendition.mp4>")
    if proxy_image_path:
        media_data.append(f"<access_rendition.largeimage>{Path(proxy_image_path).name}</access_rendition.largeimage>")
    if proxy_thumb_path:
        media_data.append(f"<access_rendition.thumbnail>{Path(proxy_thumb_path).name}</access_rendition.thumbnail>")
    if checksum_md5 and checksum_path:
        media_data.append(f"<Checksum><checksum.value>{checksum_md5}</checksum.value><checksum.type>MD5</checksum.type>")
        media_data.append(f"<checksum.date>{checksum_date}</checksum.date><checksum.path>'{file_path_str}'</checksum.path></Checksum>")
    if media_data:
        media_data.append(f"<Edit><edit.name>datadigipres</edit.name><edit.date>{str(datetime.datetime.now())[:10]}</edit.date>")
        media_data.append(f"<edit.time>{str(datetime.datetime.now())[11:19]}</edit.time>")
        media_data.append("<edit.notes>Automated bulk checksum and proxy documentation.</edit.notes></Edit>")

    if media_priref and media_data:
        cid_tic = time.perf_counter()
        success = utils.cid_media_append(media_priref, media_data)
        cid_toc = time.perf_counter()
        cid_update_time = round(cid_toc - cid_tic, 3)
        if not success:
            context.log.error(f"Proxy file names failed to write to Media priref {media_priref}")
            duration_sec = round(time.perf_counter() - tic, 3)
            return Output(None, metadata={"duration_sec": duration_sec, "preview": f"CID update failed for file {file_id}"})
        context.log.info(f"Proxy filenames updated to CID Media record: {media_priref}")
    else:
        cid_update_time = 0.0
        context.log.info(f"Skipping CID update — no media priref or proxy data for {file_name}")

    db.update_file_status(file_id, proxy_created=True)

    all_complete = db.check_all_stages_complete(file_id)

    if not all_complete:
        context.log.info(f"File {file_id}: not all stages complete yet, skipping deletion")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"Cleanup pending: file {file_id}"})

    del_time = 0.0
    if source_path.exists():
        context.log.info(f"All stages complete. Deleting source: {source_path}")
        del_tic = time.perf_counter()
        source_path.unlink()
        del_toc = time.perf_counter()
        del_time = round(del_toc - del_tic, 3)
        db.update_file_status(file_id, file_status="complete", source_deletion=True, error_message=None)
    else:
        context.log.warning(f"Source file already gone: {source_path}")
        db.update_file_status(file_id, file_status="complete", source_deletion=True, error_message=None)

    duration_sec = round(time.perf_counter() - tic, 3)

    metadata = {
        "duration_sec": duration_sec,
        "file_id": file_id,
        "cid_update_time_sec": cid_update_time,
        "delete_time_sec": del_time,
        "preview": f"File {file_id} cleanup in {duration_sec}s (CID: {cid_update_time}s, delete: {del_time}s)",
    }

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="check_and_delete_source",
            event_type="op_completed",
            status="success",
            metadata=metadata,
        )
    except Exception:
        pass

    return Output(None, metadata={
        "duration_sec": duration_sec,
        "cid_update_time_sec": cid_update_time,
        "delete_time_sec": del_time,
        "preview": f"File {file_id} cleaned in {duration_sec}s",
    })
