from dagster import job, Config
from dagster_celery import celery_executor

from autoingest.ops.encoding.proxy_video import encode_proxy_mp4
from autoingest.ops.encoding.proxy_thumbnail import generate_thumbnail
from autoingest.ops.cleanup.source_deletion import check_and_delete_source


@job(
    resource_defs={
        "workflow_db": "workflow_db",
        "encoding_config": "encoding_config",
    },
)
def encoding_job():
    proxy = encode_proxy_mp4()
    thumb = generate_thumbnail(proxy)
    check_and_delete_source(thumb)
