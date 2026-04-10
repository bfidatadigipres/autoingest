import hashlib
import json
import subprocess
import os
from dagster import op, Out


def _md5_checksum(file_path: str) -> str:
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@op(
    out={"enriched_file_info": Out(dict)},
    tags={"dagster-celery/queue": "default"},
)
def extract_metadata(context, file_info: dict) -> dict:
    file_path = file_info["file_path"]
    ffprobe_path = os.environ.get("FFPROBE_PATH", "ffprobe")

    context.log.info(f"Extracting metadata from {file_path}")

    result = subprocess.run(
        [
            ffprobe_path,
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            file_path,
        ],
        capture_output=True,
        text=True,
    )

    metadata = {}
    if result.returncode == 0:
        metadata = json.loads(result.stdout)
    else:
        context.log.warning(f"ffprobe failed: {result.stderr}")

    file_info["metadata_json"] = json.dumps(metadata)
    return file_info


@op(
    out={"checksummed_file_info": Out(dict)},
    tags={"dagster-celery/queue": "default"},
)
def generate_checksum(context, enriched_file_info: dict) -> dict:
    file_path = enriched_file_info["file_path"]
    context.log.info(f"Generating MD5 checksum for {file_path}")

    checksum = _md5_checksum(file_path)
    enriched_file_info["checksum_md5"] = checksum

    context.log.info(f"Checksum: {checksum}")
    return enriched_file_info
