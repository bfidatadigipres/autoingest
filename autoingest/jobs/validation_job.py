from dagster import job
from dagster_celery import celery_executor

from autoingest.ops.archive.verification import verify_tape_copy
from autoingest.ops.encoding.proxy_video import encode_proxy_mp4
from autoingest.ops.encoding.proxy_images import generate_images
from autoingest.ops.cleanup.source_deletion import check_and_delete_source


celery_exec = celery_executor.configured(
    {
        "broker": {"env": "CELERY_BROKER_URL"},
        "backend": {"env": "CELERY_RESULT_BACKEND"},
        "config_source": {"task_always_eager": False},
    },
    name="celery_redis",
)


@job(executor_def=celery_exec)
def validation_job():
    verified = verify_tape_copy()
    proxy = encode_proxy_mp4(verified)
    images = generate_images(proxy)
    check_and_delete_source(images)