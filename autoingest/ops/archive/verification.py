from multiprocessing import context
from xml.parsers.expat import errors
from ...resources import utils
from ...resources import bp_utils as bp
from ...resources import adlib

import os
import json
from dagster import op, Out


JSON_PATH = os.path.join(os.environ.get("LOG_PATH"), "black_pearl/")


@op(
    config_schema={"file_path": str},
    out=Out(dict),
    required_resource_keys={"workflow_db"},
)
def verify_tape_copy(context) -> dict:
    file_path = context.op_config["file_path"]
    file = os.path.basename(file_path)
    context.log.info(f"Verifying file: {file_path}")

    # Access database information
    db = context.resources.workflow_db
    file_info = db.lookup_file_details(file)

    # Check the file completed ingest / autoingest path available
    status = file_info[2]
    if status != "File cleared for ingest":
        context.log.warning(f"File has not be cleared for ingest: {status}.")
        return {}
    autoingest_path = file_info[45]
    if not autoingest_path or not os.path.exists(autoingest_path):
        context.log.warning(f"Autoingest path not found:\n{autoingest_path}")
        return {}
    json_path = retrieve_json_data(file_info[46])
    if not json_path:
        context.log.warning(f"Unable to locate file in JSON path:\n{json_path}")
        return {}

    errors = []
    validation_pass = True
    ingest_retry_needed = False

    success = check_for_failed_file(file, json_path)
    if success is False:
        validation_pass = False
        ingest_retry_needed = True
        errors.append(f"JOB ID partially failed to ingest file {file} to DPI:\n{json_path}")
        # Actions needed here to move file back into ingest path / Refresh dB entry for retry of PUT
        return {} #What?

    bp_bucket = file_info[18]
    confirmed, bp_checksum, bp_length = bp.get_confirmation_length_md5(file, bp_bucket)

    if confirmed is None or confirmed == "No object list":
        context.log.warning("Problem retrieving Black Pearl TapeList.")
        validation_pass = False
        ingest_retry_needed = True
        errors.append("Problem retrieving BlackPearl TapeList.")
    elif confirmed is False:
        context.log.warning("Assigned to storage domain is FALSE: {file}")
        validation_pass = False
        errors.append("BlackPearl has not persisted file to data tape but ObjectList exists")
    elif confirmed is True:
        context.log.info("Assigned to storage domain confirmed as TRUE")

    local_checksum = file_info[22].strip()
    if not local_checksum.lower() == bp_checksum.lower():
        context.log.warning(f"Black Pearl version of {file} has different checksum to local:\n{bp_checksum}\n{local_checksum}")
        validation_pass = False
        errors.append("Failed fixity check: checksums do not match")
    if len(local_checksum) == 32 and len(bp_checksum) == 32:
        context.log.info(f"Checksums match:\n{bp_checksum} - Black Pearl MD5\n{local_checksum} - Local checksum")

    if not bp_length:
        context.log.warning("Could not extract BlackPearl object length")
        validation_pass = False
        errors.append("Filesize does not match BlackPearl object length")
    if bp_length != file_info.get("file_size"):
        context.log.warning(f"Black pearl file length {bp_length} does not match original file length {file_info.get("file_size")}")
        validation_pass = False
        errors.append("Filesize does not match BlackPearl object length")
    else:
        context.log.info("Black Pearl object length matches file size")

    # Check for Media Rec
    mcheck = utils.check_file_has_media_rec(file)
    if mcheck is not False:
        context.log.info(
                f"Media record already exists for file: {file}"
            )
        validation_pass = False
        errors.append(f"Filename already has a CID Media record: '<{file}>'")

    # JMW up to here
    # Data at end needed to sort validation_pass False / True decision and ingest_retry_needed



    for file_id, obj in zip(file_info["file_ids"], file_info["objects"]):
        context.log.info(f"Verifying tape copy for file {file_id}: {obj['name']}")

        tape_info = spectralogic.verify_object(obj["name"])
        local_size = obj["size"]
        tape_size = int(tape_info["size"])

        # Fetch local checksum from DB for comparison
        # (already stored during metadata extraction)
        from autoingest.resources.database import WorkflowDatabase
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


def retrieve_json_data(job_id: str) -> str:
    """
    Look for matching JSON file
    """
    json_file = [x for x in os.listdir(JSON_PATH) if str(job_id) in str(x)]
    if json_file:
        return os.path.join(JSON_PATH, json_file[0])
    else:
        return None


def check_for_failed_file(file, json_file):
    """
    Check in JSON for failed file in list
    """
    failed_files = json_check(json_file)
    if not failed_files:
        return False
    for ffile in failed_files:
        for key, value in ffile.items():
            if key == "Name":
                if value == file:
                    return True
    return False


def json_check(json_pth: str) -> Optional[str]:
    """
    Open json and return value for ObjectsNotPersisted
    """
    with open(json_pth) as file:
        dct = json.load(file)
        
    for k, v in dct.items():
        if k == "Notification":
            notifications = v
    if isinstance(notifications, dict):
        for ky, vl in notifications.items():
            if ky == "Event":
                events = vl
    else:
        return None
    if isinstance(events, dict):
        for key, val in events.items():
            if key == "ObjectsNotPersisted":
                return val
    else:
        return None

