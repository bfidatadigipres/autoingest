import os
from types import SimpleNamespace
from dagster import resource, InitResourceContext


@resource
def encoding_config(context: InitResourceContext):
    return SimpleNamespace(
        ffmpeg_path=os.environ.get("FFMPEG_PATH", "/usr/bin/ffmpeg"),
        ffprobe_path=os.environ.get("FFPROBE_PATH", "/usr/bin/ffprobe"),
        thread_count=int(os.environ.get("ENCODING_THREAD_COUNT", "0")),
        proxy_output_path=os.environ.get("PROXY_OUTPUT_PATH", ""),
    )