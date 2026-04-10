import os
import subprocess
from pathlib import Path
from dagster import op, Out


@op(
    out={"thumbnail_result": Out(dict)},
    tags={"dagster-celery/queue": "encoding"},
)
def generate_thumbnail(context, proxy_result: dict, encoding_config) -> dict:
    proxy_path = proxy_result["proxy_path"]
    filename_stem = Path(proxy_path).stem.replace("_proxy", "")
    output_dir = Path(encoding_config.proxy_output_path) / "thumbnails"
    output_path = output_dir / f"{filename_stem}_thumb.jpg"

    output_dir.mkdir(parents=True, exist_ok=True)

    context.log.info(f"Generating thumbnail from proxy: {proxy_path}")

    cmd = [
        encoding_config.ffmpeg_path,
        "-i", proxy_path,
        "-vf", "thumbnail,scale=640:-1",
        "-frames:v", "1",
        "-y",
        str(output_path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        context.log.error(f"Thumbnail generation failed: {result.stderr}")
        raise RuntimeError(f"Thumbnail generation failed for {proxy_path}")

    context.log.info(f"Thumbnail created: {output_path}")

    return {
        "file_id": proxy_result.get("file_id"),
        "proxy_path": proxy_path,
        "thumbnail_path": str(output_path),
    }
