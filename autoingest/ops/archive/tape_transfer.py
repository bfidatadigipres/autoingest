"""
Sort all ingestible files into
batches per storage and move into
ingest_<date> folder the initiate PUSH
irrespective of smaller size. Limit to
1 TB max batch - iterate multiple PUTs if needed
>1TB used blobbed PUT for single operations
"""

from dagster import op, Out


ONE_TB = 1_099_511_627_776  # 1 TiB in bytes


@op(
    out={"batch_result": Out(dict)},
    tags={"dagster-celery/queue": "default"}, # Remove this - must be one
)
def batch_transfer_to_tape(context, workflow_db, spectralogic):
    pending = workflow_db.get_pending_tape_files(max_bytes=ONE_TB)

    if not pending:
        context.log.info("No files pending for tape transfer")
        return {"job_id": None, "file_ids": [], "objects": []}

    context.log.info(f"Batching {len(pending)} files for tape transfer")

    file_list = []
    file_ids = []
    for row in pending:
        file_id, filepath, filesize, checksum = row
        object_name = filepath.replace("/", "_").lstrip("_")
        file_list.append({
            "name": object_name,
            "path": filepath,
            "size": filesize,
        })
        file_ids.append(file_id)

    job_id = spectralogic.put_bulk(file_list)
    context.log.info(f"SpectraLogic job submitted: {job_id}")

    # Mark files as tape_pending
    for fid in file_ids:
        workflow_db.update_file_status(fid, status="tape_pending", tape_job_id=job_id)

    return {
        "job_id": job_id,
        "file_ids": file_ids,
        "objects": file_list,
    }
