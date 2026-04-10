import os
from dagster import op, Out


@op(
    out={"verification_results": Out(list)},
    tags={"dagster-celery/queue": "default"},
)
def verify_tape_copies(context, batch_result: dict, workflow_db, spectralogic):
    if not batch_result["file_ids"]:
        context.log.info("No files to verify")
        return []

    results = []
    for file_id, obj in zip(batch_result["file_ids"], batch_result["objects"]):
        context.log.info(f"Verifying tape copy for file {file_id}: {obj['name']}")

        tape_info = spectralogic.verify_object(obj["name"])
        local_size = obj["size"]
        tape_size = int(tape_info["size"])

        # Fetch local checksum from DB for comparison
        # (already stored during metadata extraction)
        from media_pipeline.resources.database import WorkflowDatabase
        with workflow_db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT checksum_md5 FROM file_catalogue WHERE id = %s",
                    (file_id,),
                )
                local_checksum = cur.fetchone()[0]

        size_match = local_size == tape_size
        checksum_match = local_checksum == tape_info["checksum"]
        verified = size_match and checksum_match

        if verified:
            workflow_db.update_file_status(
                file_id, tape_verified=True, status="tape_verified"
            )
            context.log.info(f"File {file_id} verified successfully")
        else:
            workflow_db.update_file_status(file_id, status="tape_verify_failed")
            context.log.error(
                f"File {file_id} verification FAILED. "
                f"Size match: {size_match}, Checksum match: {checksum_match}"
            )

        results.append({"file_id": file_id, "verified": verified})

    return results
