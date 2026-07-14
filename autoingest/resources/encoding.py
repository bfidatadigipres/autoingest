import os
from dagster import resource, InitResourceContext


class EncodingConfig:
    def __init__(self):
        self.ffmpeg_path = os.environ.get("FFMPEG_PATH", "/usr/bin/ffmpeg")
        self.ffprobe_path = os.environ.get("FFPROBE_PATH", "/usr/bin/ffprobe")
        self.proxy_output_path = os.environ.get("PROXY_OUTPUT_PATH", "/mnt/proxy")


@resource
def encoding_config(context: InitResourceContext) -> EncodingConfig:
    return EncodingConfig()