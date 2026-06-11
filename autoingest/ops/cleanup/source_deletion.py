import os
from pathlib import Path
from dagster import op, OpExecutionContext
from ...resources import utils
from ...resources import adlib


@op(required_resource_keys={"workflow_db"})
def check_and_delete_source(
    context: OpExecutionContext,
    file_info: dict,
):
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
    success = utils.cid_media_append(media_priref, media_data)
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
        return

    # Retrieve the source path
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_path FROM file_catalogue WHERE id = %s", (file_id,)
            )
            row = cur.fetchone()

    if row is None:
        context.log.error(f"File {file_id} not found in catalogue")
        return

    source_path = Path(row[0])
    if source_path.exists():
        context.log.info(f"All stages complete. Deleting source: {source_path}")
        source_path.unlink()
        db.update_file_status(file_id, status="complete", source_deleted=True)
    else:
        context.log.warning(f"Source file already gone: {source_path}")
        db.update_file_status(file_id, status="complete", source_deleted=True)
