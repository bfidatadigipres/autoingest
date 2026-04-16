import os
from pathlib import Path
from dagster import op, Out


@op(
    tags={"dagster-celery/queue": "default"},
)
def check_and_delete_source(
    context,
    thumbnail_result: dict,
    workflow_db
):
    file_id = thumbnail_result["file_id"]
    
    # Update proxy data to Media dB
    media_data = []
    proxy_video_path = thumbnail_result["proxy_video_path"]
    proxy_image_path = thumbnail_result["proxy_image_path"]
    proxy_thumb_path = thumbnail_result["proxy_thumb_path"]
    media_data.append(
        f"<access_rendition.mp4>{os.path.split(proxy_video_path)[1]}</access_rendition.mp4>",
        f"<access_rendition.largeimage>{os.path.split(proxy_image_path)[1]}</access_rendition.largeimage>",
        f"<access_rendition.thumbnail>{os.path.split(proxy_thumb_path)[1]}</access_rendition.thumbnail>"
    )
    media_priref = thumbnail_result["cid_media_priref"]
    
    # Update all proxy file paths here
    workflow_db.update_file_status(file_id, proxy_created=True)

    # Check if all stages are complete
    all_complete = workflow_db.check_all_stages_complete(file_id)

    if not all_complete:
        context.log.info(
            f"File {file_id}: not all stages complete yet, skipping deletion"
        )
        return

    # Retrieve the source path
    with workflow_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT filepath FROM file_catalogue WHERE id = %s", (file_id,)
            )
            row = cur.fetchone()

    if row is None:
        context.log.error(f"File {file_id} not found in catalogue")
        return

    source_path = Path(row[0])
    if source_path.exists():
        context.log.info(f"All stages complete. Deleting source: {source_path}")
        source_path.unlink()
        workflow_db.update_file_status(file_id, status="complete", source_deleted=True)
    else:
        context.log.warning(f"Source file already gone: {source_path}")
        workflow_db.update_file_status(file_id, status="complete", source_deleted=True)
