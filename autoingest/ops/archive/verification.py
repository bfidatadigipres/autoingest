from ...resources import utils
from ...resources import bp_utils as bp
from ...resources import adlib

import os
from dagster import op, OpExecutionContext


@op(
    required_resource_keys={"workflow_db"},
    config_schema={"file_path": str},
    tags={"dagster-celery/queue": "default"},
)
def verify_tape_copy(context: OpExecutionContext) -> dict:
    file_path = context.op_config["file_path"]
    file = os.path.basename(file_path)
    context.log.info(f"Verifying file: {file_path}")

    # Access database information
    db = context.resources.workflow_db
    file_info = db.lookup_file_details(file)

    # Start validation checks
    status = file_info[2]
    if status != "File cleared for ingest":
        context.log.warning(f"File has not be cleared for ingest: {status}.")
        return {}

    errors = []
    validation_pass = True

    bp_bucket = file_info[18]
    bp_checksum = bp.get_bp_md5(file, bp_bucket)
    local_checksum = file_inf0[22]
    if not local_checksum.lower() == bp_checksum.lower():
        context.log.warning(f"Black Pearl version of {file} has different checksum to local:\n{bp_checksum}\n{local_checksum}")
        validation_pass = False
        errors.append("Failed fixity check: checksums do not match")
        return {}

    if len(local_checksum) == 32 and len(bp_checksum) == 32:
        context.log.info(f"Checksums match:\n{bp_checksum} - Black Pearl MD5\n{local_checksum} - Local checksum")


    # JMW up to here



    for file_id, obj in zip(file_info["file_ids"], file_info["objects"]):
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



