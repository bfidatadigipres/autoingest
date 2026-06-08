import os
from dagster import resource, InitResourceContext


class EncodingConfig:
    def __init__(self):
        self.ffmpeg_path = os.environ.get("FFMPEG_PATH", "/usr/bin/ffmpeg")
        self.ffprobe_path = os.environ.get("FFPROBE_PATH", "/usr/bin/ffprobe")
        self.thread_count = int(os.environ.get("ENCODING_THREAD_COUNT", "0"))
        self.proxy_output_path = os.environ.get("PROXY_OUTPUT_PATH", "/mnt/proxy")


@resource
def encoding_config(context: InitResourceContext) -> EncodingConfig:
    return EncodingConfig()