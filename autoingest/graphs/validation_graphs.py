from dagster import graph

from autoingest.ops.local.verification import verify_tape_copy
from autoingest.ops.local.source_deletion import check_and_delete_source
from autoingest.ops.local.cid_metadata_update import update_cid_metadata
from autoingest.ops.celery.proxy_video import encode_proxy_mp4
from autoingest.ops.celery.proxy_images import generate_images


@graph
def verify_local_graph():
    return verify_tape_copy()


@graph
def encoding_celery_graph():
    proxy_info = encode_proxy_mp4()
    images = generate_images(proxy_info)
    return images


@graph
def generate_images_graph():
    return generate_images()


@graph
def cleanup_graph():
    return check_and_delete_source()


@graph
def metadata_update_graph():
    return update_cid_metadata()
