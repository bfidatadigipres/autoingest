import os
from dagster import op, OpExecutionContext


# ops/archive/verification.py
@op(
    required_resource_keys={"workflow_db"},
    config_schema={"file_path": str},
    tags={"dagster-celery/queue": "default"},
)
def verify_tape_copy(context: OpExecutionContext) -> dict:
    file_path = context.op_config["file_path"]
    # ... verify the tape copy, write result to DB
    return {"file_path": file_path, "file_id": ..., ...}

# ops/encoding/proxy_video.py
@op(
    required_resource_keys={"workflow_db", "encoding_config"},
    tags={"dagster-celery/queue": "encoding"},
)
def encode_proxy_mp4(context: OpExecutionContext, file_info: dict) -> dict:
    cfg = context.resources.encoding_config
    # ... create proxy MP4 using cfg.ffmpeg_path
    return {**file_info, "proxy_video_path": ...}

# ops/encoding/proxy_images.py
@op(
    required_resource_keys={"workflow_db", "encoding_config"},
    tags={"dagster-celery/queue": "encoding"},
)
def generate_images(context: OpExecutionContext, file_info: dict) -> dict:
    # ... generate proxy images
    return {**file_info, "proxy_image_path": ...}

# ops/cleanup/source_deletion.py
@op(
    required_resource_keys={"workflow_db"},
    tags={"dagster-celery/queue": "default"},
)
def check_and_delete_source(context: OpExecutionContext, file_info: dict):
    db = context.resources.workflow_db
    # ... check all stages complete, delete source file