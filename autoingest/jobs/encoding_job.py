from dagster import job, Config
from dagster_celery import celery_executor

from autoingest.ops.encoding.proxy_video import encode_proxy_mp4
from autoingest.ops.encoding.proxy_images import generate_thumbnail
from autoingest.ops.encoding.proxy_images import generate_largeimage
from autoingest.ops.cleanup.source_deletion import check_and_delete_source


@job(
    resource_defs={
        "workflow_db": "workflow_db",
        "encoding_config": "encoding_config",
    },
)
def encoding_job():
    proxy = encode_proxy_mp4()
    image = generate_largeimage(proxy)
    thumb = generate_thumbnail(image)
    check_and_delete_source(thumb)
