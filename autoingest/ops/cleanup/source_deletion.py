import os
import time
from pathlib import Path
from dagster import op, OpExecutionContext, Output
from ...resources import utils
from ...resources import adlib


@op(required_resource_keys={"workflow_db"})
def check_and_delete_source(
    context: OpExecutionContext,
    file_info: dict,
) -> Output:
    tic = time.perf_counter()
    file_id = file_info["file_id"]
    
    # Update proxy data to Media dB
    media_data = []
    proxy_video_path = file_info["proxy_video_path"]
    proxy_image_path = file_info["proxy_image_path"]
    proxy_thumb_path = file_info["proxy_thumb_path"]
    media_data.append(
        f"<access_rendition.mp4>{os.path.split(proxy_video_path)[1]}</access_rendition.mp4>"
    )
    media_data.append(
        f"<access_rendition.largeimage>{os.path.split(proxy_image_path)[1]}</access_rendition.largeimage>"
    )
    media_data.append(
        f"<access_rendition.thumbnail>{os.path.split(proxy_thumb_path)[1]}</access_rendition.thumbnail>"
    )
    media_priref = file_info["cid_media_priref"]
    cid_tic = time.perf_counter()
    success = utils.cid_media_append(media_priref, media_data)
    cid_toc = time.perf_counter()
    cid_update_time = round(cid_toc - cid_tic, 3)
    if not success:
        context.log.error(f"Proxy file names failed to write to Media priref {media_priref}")
        raise RuntimeError("Proxy file names failed to write to CID digital media record")
    context.log.info(f"Proxy filenames updated to CID Media record: {media_priref}")

    # Update all proxy file paths here
    db = context.resources.workflow_db
    db.update_file_status(file_id, proxy_created=True)

    # Check if all stages are complete
    all_complete = db.check_all_stages_complete(file_id)

    if not all_complete:
        context.log.info(
            f"File {file_id}: not all stages complete yet, skipping deletion"
        )
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output(None, metadata={"duration_sec": duration_sec, "preview": f"Cleanup pending: file {file_id}"})

    # Retrieve the source path
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_path FROM app.file_catalogue WHERE id = %s", (file_id,)
            )
            row = cur.fetchone()

    if row is None:
        context.log.error(f"File {file_id} not found in catalogue")
        return Output(None, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

    source_path = Path(row[0])
    del_time = 0.0
    if source_path.exists():
        context.log.info(f"All stages complete. Deleting source: {source_path}")
        del_tic = time.perf_counter()
        source_path.unlink()
        del_toc = time.perf_counter()
        del_time = round(del_toc - del_tic, 3)
        db.update_file_status(file_id, file_status="complete", source_deletion=True)
    else:
        context.log.warning(f"Source file already gone: {source_path}")
        db.update_file_status(file_id, file_status="complete", source_deletion=True)

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
