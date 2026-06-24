import os
import time
from pathlib import Path
from typing import Any

import autoingest.resources.proxy_utils as ut
from dagster import op, OpExecutionContext, Output


@op(
    required_resource_keys={"workflow_db", "encoding_config"},
    tags={"dagster-celery/queue": "encoding"},
)
def generate_images(
    context: OpExecutionContext,
    file_info: dict[str, Any],
) -> Output:
    tic = time.perf_counter()
    proxy_path = file_info["proxy_video_path"]
    root = os.path.split(proxy_path)[0]
    filename_stem = Path(proxy_path).stem
    file_id = file_info.get("file_id")
    file_name = Path(file_info["file_path"]).name

    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_status FROM app.file_catalogue "
                "WHERE file_name = %s ORDER BY created_at DESC LIMIT 1",
                (file_name,),
            )
            row = cur.fetchone()

    if not row:
        context.log.error(f"No DB record found for {file_name}")
        return Output({
            "file_id": file_id,
            "proxy_video_path": proxy_path,
            "proxy_image_path": "",
            "proxy_thumb_path": "",
        }, metadata={"duration_sec": round(time.perf_counter() - tic, 3), "preview": f"No record: {file_name}"})

    file_status = row[0]

    if file_status != "creating_images":
        context.log.info(
            f"File {file_name} has status '{file_status}' — expected 'creating_images'. Skipping."
        )
        return Output({
            "file_id": file_id,
            "proxy_video_path": proxy_path,
            "proxy_image_path": "",
            "proxy_thumb_path": "",
        }, metadata={"duration_sec": round(time.perf_counter() - tic, 3), "preview": f"Skipped: status={file_status}"})

    mime = file_info["mime_type"]
    if mime not in ["video", "image"]:
        context.log.info("MIME type is not Video/Image and cannot be converted...")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output({
            "file_id": file_info.get("file_id"),
            "proxy_video_path": proxy_path,
            "proxy_image_path": "",
            "proxy_thumb_path": "",
        }, metadata={"duration_sec": duration_sec, "preview": "Skipped (non-media)"})

    source = file_info.get("source")
    if source.lower() in ["netflix", "amazon", "disney"]:
        context.log.info(f"Source is {source}... No transcode required.")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output({
            "file_id": file_info.get("file_id"),
            "proxy_video_path": proxy_path,
            "proxy_image_path": "",
            "proxy_thumb_path": "",
        }, metadata={"duration_sec": duration_sec, "preview": f"Skipped (non-BFI): {source}"})

    if mime == "video":
        source_image = os.path.join(root, f"{filename_stem}.jpg")
        if not os.path.isfile(source_image):
            context.log.error(f"JPEG extraction of proxy file not found: {source_image}")
            raise RuntimeError("JPEG image not found to create image/thumbnail")
    elif mime == "image":
        source_image = file_info["file_path"]

    context.log.info(f"Mime type is {mime} and source image is {source_image}")
    largeimage_path = os.path.join(root, f"{filename_stem}_largeimage.jpg")
    thumbnail_path = os.path.join(root, f"{filename_stem}_thumbnail.jpg")

    percent = ""
    oversize = False
    if mime == "video":
        oversize = False
    else:
        context.log.info("Generating large (full size copy) and thumbnail jpeg images.")

        size = os.path.getsize(source_image)
        if 104857600 <= int(size) <= 209715200:
            context.log.info("Image is over 100MB. Applying resize to large image.")
            percent = "75"
            oversize = True
        elif 209715201 <= int(size) <= 314572800:
            context.log.info("Image is over 200MB. Applying resize to large image.")
            percent = "60"
            oversize = True
        elif 314572801 <= int(size) <= 419430400:
            context.log.info("Image is over 300MB. Applying resize to large image.")
            percent = "45"
            oversize = True
        elif int(size) > 419430401:
            context.log.info("Image is over 400MB. Applying resize to large image.")
            percent = "30"
            oversize = True

    context.log.info(f"Generating largeimage from proxy: {proxy_path}")
    img_tic = time.perf_counter()
    if oversize is False:
        proxy_image_path = ut.make_jpg(source_image, "full", largeimage_path, None)
    else:
        proxy_image_path = ut.make_jpg(source_image, "oversize", largeimage_path, percent)
    context.log.info(f"Generating thumbnail from proxy: {proxy_path}")
    proxy_thumb_path = ut.make_jpg(source_image, "thumb", thumbnail_path, None)
    img_toc = time.perf_counter()
    image_time = round(img_toc - img_tic, 3)

    if proxy_thumb_path is None:
        proxy_thumb_path = ""
    if proxy_image_path is None:
        proxy_image_path = ""
    if os.path.isfile(proxy_image_path) and os.path.isfile(proxy_thumb_path):
        context.log.info(f"New images created:\n - {proxy_image_path}\n - {proxy_thumb_path}")
    else:
        context.log.error(f"One or both JPEG image creations failed for file {Path(proxy_path).name}")
        raise RuntimeError("JPEG proxy image failed for image and/or thumbnail")

    if mime == "video":
        context.log.info("Cleaning up Video proxy filename / JPEG export")
        os.replace(proxy_path, os.path.join(root, filename_stem))
        os.remove(os.path.join(root, f"{filename_stem}.jpg"))

    context.log.info("Updating Proxy Image data to dB")
    db = context.resources.workflow_db
    db.update_file_status(
        file_info.get("file_id"),
        proxy_image_path=proxy_image_path,
        proxy_thumb_path=proxy_thumb_path,
        image_time_sec=image_time,
    )

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)

    large_size = os.path.getsize(proxy_image_path) if os.path.isfile(proxy_image_path) else 0
    thumb_size = os.path.getsize(proxy_thumb_path) if os.path.isfile(proxy_thumb_path) else 0

    metadata = {
        "duration_sec": duration_sec,
        "file_name": filename_stem,
        "image_time_sec": image_time,
        "large_image_size": large_size,
        "thumb_size": thumb_size,
        "oversize": oversize,
        "preview": f"{filename_stem} images in {image_time}s (large: {large_size}B, thumb: {thumb_size}B)",
    }

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="generate_images",
            event_type="op_completed",
            status="success",
            metadata=metadata,
        )
    except Exception:
        pass

    file_name = Path(file_info["file_path"]).name
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app.file_catalogue SET file_status = 'encoded', error_message = NULL, updated_at = NOW() "
                "WHERE file_name = %s",
                (file_name,),
            )

    return Output({
        "file_id": file_info.get("file_id"),
        "proxy_video_path": proxy_path,
        "proxy_image_path": proxy_image_path,
        "proxy_thumb_path": proxy_thumb_path,
    }, metadata={
        "duration_sec": duration_sec,
        "image_time_sec": image_time,
        "large_image_size": large_size,
        "preview": f"{filename_stem} images in {image_time}s",
    })
