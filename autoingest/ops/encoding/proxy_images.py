import os
import autoingest.resources.proxy_utils as ut
from pathlib import Path
from dagster import op, OpExecutionContext


@op(
    required_resource_keys={"workflow_db", "encoding_config"},
    tags={"dagster-celery/queue": "encoding"},    
)
def generate_images(
    context: OpExecutionContext,
    file_info: dict,
) -> dict:
    proxy_path = file_info["proxy_video_path"]
    root = os.path.split(proxy_path)[0]
    filename_stem = Path(proxy_path).stem

    # Check file type first
    mime = file_info["mime_type"]
    if mime not in ["video", "image"]:
        context.log.info("MIME type is not Video/Image and cannot be converted...")
        return {
            "file_id": file_info.get("file_id"),
            "proxy_video_path": proxy_path,
            "proxy_image_path": "",
            "proxy_thumb_path": "",
        }
    # Check and block non-BFI sources
    source = file_info.get("source")
    if source.lower() in ["netflix", "amazon", "disney"]:
        context.log.info(f"Source is {source}... No transcode required.")
        return {
            "file_id": file_info.get("file_id"),
            "proxy_video_path": proxy_path,
            "proxy_image_path": "",
            "proxy_thumb_path": "",
        }

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
    if mime == "video":
        oversize = False
    else:
        oversize = False
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
    if oversize is False:
        proxy_image_path = ut.make_jpg(source_image, "full", largeimage_path, None)
    else:
        proxy_image_path = ut.make_jpg(source_image, "oversize", largeimage_path, percent)
    context.log.info(f"Generating thumbnail from proxy: {proxy_path}")
    proxy_thumb_path = ut.make_jpg(source_image, "thumb", thumbnail_path, None)

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
        {
            "proxy_image_path": proxy_image_path,
            "proxy_thumb_path": proxy_thumb_path,
        }
    )

    return {
        "file_id": file_info.get("file_id"),
        "proxy_video_path": proxy_path,
        "proxy_image_path": proxy_image_path,
        "proxy_thumb_path": proxy_thumb_path,
    }