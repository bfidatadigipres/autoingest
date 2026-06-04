from dagster import job

from autoingest.resources.celery_client import celery_executor_configured
from autoingest.ops.archive.verification import verify_tape_copy
from autoingest.ops.encoding.proxy_video import encode_proxy_mp4
from autoingest.ops.encoding.proxy_images import generate_images
from autoingest.ops.cleanup.source_deletion import check_and_delete_source


@job(executor_def=celery_executor_configured)
def validation_job():
    verified = verify_tape_copy()
    proxy = encode_proxy_mp4(verified)
    images = generate_images(proxy)
    check_and_delete_source(images)