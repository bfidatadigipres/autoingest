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
    root, file = os.path.split(file_path)
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
    deletion_needed = False
    results = {}

    success = check_for_failed_file(file, json_path)
    if success is False:
        validation_pass = False
        ingest_retry_needed = True
        errors.append(f"JOB ID partially failed to ingest file {file} to DPI:\n{json_path}")
        # Actions needed here to move file back into ingest path / Refresh dB entry for retry of PUT

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
        ingest_retry_needed = True
        errors.append("BlackPearl has not persisted file to data tape but ObjectList exists")
    elif confirmed is True:
        context.log.info("Assigned to storage domain confirmed as TRUE")
        results["persisted_ok"] = confirmed

    local_checksum = file_info[22].strip()
    if local_checksum.lower() != bp_checksum.lower():
        context.log.warning(f"Black Pearl version of {file} has different checksum to local:\n{bp_checksum}\n{local_checksum}")
        validation_pass = False
        deletion_needed = True
        ingest_retry_needed = True
        errors.append("Failed fixity check: checksums do not match")
    if len(local_checksum) == 32 and len(bp_checksum) == 32:
        context.log.info(f"Checksums match:\n{bp_checksum} - Black Pearl MD5\n{local_checksum} - Local checksum")
        results["bp_etag"] = bp_checksum

    if not bp_length:
        context.log.warning("Could not extract BlackPearl object length")
        validation_pass = False
        errors.append("Filesize does not match BlackPearl object length")
    if bp_length != file_info[20].strip():
        context.log.warning(f"Black pearl file length {bp_length} does not match original file length {file_info[20]}")
        validation_pass = False
        deletion_needed = True
        ingest_retry_needed = True
        errors.append("Filesize does not match BlackPearl object length")
    else:
        context.log.info("Black Pearl object length matches file size")
    results["bp_length"] = bp_length

    bp_version = bp.get_version_id(file)
    if len(bp_version) > 30:
        results["bp_version_id"] = bp_version
        context.log.info(f"Version ID for the file found: {bp_version}")
    else:
        context.log.warning(f"Could not retrieve Version ID from BlackPearl for file {file}")

    # Check for Media Rec
    mcheck = utils.check_file_has_media_rec(file)
    if mcheck is not False:
        context.log.info(
                f"Media record already exists for file: {file}"
            )
        validation_pass = False
        errors.append(f"Filename already has a CID Media record: '<{file}>'")

    # Check if file should be deleted / reingested
    if validation_pass is False:
        if deletion_needed is True:
            context.log.warning(f"{file} did not ingest cleanly into BlackPearl, so deleting file with version ID {bp_version}")
            delete_confirm = bp.delete_black_pearl_object(file, bp_version, file_info[18])
            if delete_confirm:
                context.log.info(f"Deletion confirmation: {delete_confirm}")
                ingest_retry_needed = True
            else:
                if not bp.etag_deletion_confirmation(file, file_info[18]):
                    context.log.info(f"Deletion confirmation received, etag absent for file {file}")
                    ingest_retry_needed = True
                else:
                    context.log.warning(f"{file} - failed to delete from Black Pearl. Manual clean up needed")
                    errors.append(f"Failed to delete Black Pearl file {file}. Manual clean up needed.")
                    ingest_retry_needed = False
        if ingest_retry_needed is True:
            success, log = utils.move_file(file_path, file_info[3])
            if success is True:
                context.log.info(log)
                results["error_message"] = "Reingest requested after failed PUT attempt"
                results["do_ingest"] = True
                return results
            else:
                context.log.warning(f"Move error for {file_info['file_name']}:\n{err}")
                results["error_message"] = "Reingest requested - manual move of file to ingest path needed"
                results["do_ingest"] = True
                return results

        # Capture all failures
        context.log.warning(f"Ingest verification failed: {errors[0]}")
        results["error_message"] = errors[0]
        results["do_ingest"] = False
        return results

    # JMW up to here - validation passed, make cid record and pass data to proxy script
    


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

