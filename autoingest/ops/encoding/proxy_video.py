import os
import utils
import subprocess
from pathlib import Path
from dagster import op, Out


@op(
    out={"proxy_result": Out(dict)},
    tags={"dagster-celery/queue": "encoding"},
)
def encode_proxy_mp4(context, file_info: dict, encoding_config) -> dict:
    source_path = file_info["file_path"]
    filename_stem = Path(source_path).stem
    filename = Path(source_path).name
    output_dir = Path(encoding_config.proxy_output_path)
    # Get input date from Media dB here for path
    input_date = utils.get_media_input_date(filename)
    output_path = output_dir / f"{input_date}" / f"{filename_stem}.mp4"

    output_dir.mkdir(parents=True, exist_ok=True)

    context.log.info(
        f"Encoding proxy MP4: {source_path} -> {output_path} "
        f"(threads: {encoding_config.thread_count})"
    )

    cmd = [
        encoding_config.ffmpeg_path,
        "-i", source_path,
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        "-threads", str(encoding_config.thread_count),
        "-y",
        str(output_path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        context.log.error(f"FFmpeg stderr: {result.stderr}")
        raise RuntimeError(f"FFmpeg encoding failed for {source_path}")

    proxy_size = output_path.stat().st_size
    context.log.info(f"Proxy created: {output_path} ({proxy_size} bytes)")

    return {
        "file_id": file_info.get("file_id"),
        "source_path": source_path,
        "proxy_path": str(output_path),
        "proxy_size": proxy_size,
    }
