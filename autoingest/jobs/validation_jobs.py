from dagster_celery import celery_executor

from autoingest.graphs.validation_graphs import (
    verify_local_graph,
    encoding_celery_graph,
    generate_images_graph,
    cleanup_graph,
    metadata_update_graph,
)

celery_exec = celery_executor.configured(
    {
        "broker": {"env": "CELERY_BROKER_URL"},
        "backend": {"env": "CELERY_RESULT_BACKEND"},
        "config_source": {"task_always_eager": False},
    },
    name="celery_redis",
)

verify_local_job = verify_local_graph.to_job(
    name="verify_local_job",
    description="Runs verify_tape_copy locally on DATA15. "
                "Sets file_status = 'verified' on success.",
)

encoding_celery_job = encoding_celery_graph.to_job(
    name="encoding_celery_job",
    executor_def=celery_exec,
    description="Runs encode_proxy_mp4 + generate_images on Celery encoding workers. "
                "Sets file_status = 'encoded' on success.",
)

generate_images_job = generate_images_graph.to_job(
    name="generate_images_job",
    executor_def=celery_exec,
    description="Runs generate_images on Celery encoding workers for files "
                "stuck at 'encoded' status. Sets file_status = 'encoding_complete' on success.",
)

cleanup_local_job = cleanup_graph.to_job(
    name="cleanup_local_job",
    description="Runs check_and_delete_source locally on DATA15. "
                "Sets file_status = 'complete' on success.",
)

metadata_update_local_job = metadata_update_graph.to_job(
    name="metadata_update_local_job",
    description="Enriches CID media record with technical metadata from MediaInfo/ExifTool. "
                "Sets file_status = 'metadata_updated' on success.",
)
