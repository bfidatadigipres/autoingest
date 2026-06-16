from ...resources import utils
from ...resources import bp_utils as bp
from ...resources import adlib

import os
import time
import json
from datetime import datetime
from typing import Optional
from dagster import op, Out, Output

JSON_PATH = os.path.join(os.environ.get("LOG_PATH", ""), "black_pearl/")
CID_API = os.environ.get("CID_API4")


@op(
    config_schema={"file_path": str},
    out=Out(dict),
    required_resource_keys={"workflow_db"},
)
def verify_tape_copy(context) -> Output:
    tic = time.perf_counter()

    file_path_str = context.op_config["file_path"]
    root, file = os.path.split(file_path_str)
    context.log.info(f"Verifying file: {file_path_str} in path {root}")

    # Access database information
    db = context.resources.workflow_db
    file_info = db.lookup_file_details(file)

    # Check the file completed ingest / autoingest path available
    status = file_info[2]
    if status != "File cleared for ingest":
        context.log.warning(f"File has not be cleared for ingest: {status}.")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})
    autoingest_path = file_info[45]
    if not autoingest_path or not os.path.exists(autoingest_path):
        context.log.warning(f"Autoingest path not found:\n{autoingest_path}")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})
    folder_number = os.path.basename(root)
    if folder_number != file_info[46]:
        context.log.error(f"Ingest folder job ID does not match that stored for file: {file_info[46]}")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})
    json_path = retrieve_json_data(file_info[46])
    if not json_path:
        context.log.warning(f"Unable to locate file in JSON path:\n{json_path}")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

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

    bp_bucket = file_info[18]
    bp_tic = time.perf_counter()
    confirmed, bp_checksum, bp_length = bp.get_confirmation_length_md5(file, bp_bucket)
    bp_toc = time.perf_counter()
    bp_check_time = round(bp_toc - bp_tic, 3)

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

    if file_info[47] == "Grouped":
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

    elif "blobbing" in file_info[18]:
        context.log.info("Blobbed file identified. Downloading for MD5 verification")
        download_path = os.path.join(root, f"downloads/{file}")
        dl_tic = time.perf_counter()
        download_id = bp.download_blobbed_object(file, download_path, file_info[18])
        dl_toc = time.perf_counter()
        context.log.info(f"Confirmed download: {download_id} ({round(dl_toc - dl_tic, 1)}s)")

        # MD5 creation / length creation
        filesize = file_path_stat = os.stat(file_path_str).st_size
        if filesize == file_info[20]:
            context.log.info(f"Downloaded file size matches source file length: {filesize}")
            results["bp_length"] = filesize
        else:
            context.log.warning(f"File length mismatch for downloaded file {file}:\n{filesize} - Downloaded length\n{file_info[20]} - Source length")
            validation_pass = False
            ingest_retry_needed = True
            deletion_needed = True

        download_hash = utils.create_md5_65536(file_path_str)
        if download_hash.lower() == file_info[21].lower():
            context.log.info(f"Checksums match between source file and downloaded:\n{download_hash} - Downloaded\n{file_info[22]} - Source file")
            results["bp_etag"] = download_hash
        else:
            context.log.warning(f"Checksum mismatch for downloaded file {file}:\n{download_hash} - Downloaded\n{file_info[22]} - Source file")
            validation_pass = False
            ingest_retry_needed = True
            deletion_needed = True

        # Clean up downloaded file
        context.log.info(f"Checks complete. Deleting download file: {download_path}")
        os.remove(download_path)

    bp_version = bp.get_version_id(file)
    if len(bp_version) > 30:
        results["bp_version_id"] = bp_version
        context.log.info(f"Version ID for the file found: {bp_version}")
    else:
        context.log.warning(f"Could not retrieve Version ID from BlackPearl for file {file}")

    # Check for existing Media Rec
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
            success_move, log = utils.move_file(file_path_str, file_info[3])
            duration_sec = round(time.perf_counter() - tic, 3)
            if success_move is True:
                context.log.info(log)
                results["error_message"] = "Reingest requested after failed PUT attempt"
                results["do_ingest"] = True
                results["validate"] = False
            else:
                context.log.warning(f"Move error for {file_info['file_name']}:\n{err}")
                results["error_message"] = "Reingest requested - manual move of file to ingest path needed"
                results["do_ingest"] = True
                results["validate"] = False
            _record_verify_event(context, db, file, duration_sec, "reingest", results)
            return Output(results, metadata={"duration_sec": duration_sec, "file_name": file, "preview": f"Reingest: {file}"})

        # Capture all failures
        context.log.warning(f"Ingest verification failed: {errors[0]}")
        results["error_message"] = errors[0]
        results["do_ingest"] = False
        results["validate"] = False
        duration_sec = round(time.perf_counter() - tic, 3)
        _record_verify_event(context, db, file, duration_sec, "failure", results)
        return Output(results, metadata={"duration_sec": duration_sec, "file_name": file, "preview": f"Verification failed: {file}"})

    # JMW up to here - validation passed, make cid record and pass data to proxy script
    context.log.info(f"Creating new CID media record for file {file}")
    cid_tic = time.perf_counter()
    media_priref = create_media_record(file_info)
    cid_toc = time.perf_counter()
    cid_time = round(cid_toc - cid_tic, 3)

    if len(media_priref) > 6:
        context.log.info(f"New media record created for ingested file: {media_priref}")
        results["cid_media_priref"] = media_priref
        results["validated"] = True
    else:
        context.log.warning(f"Failed to create media record for ingested file: {file}")
        results["validated"] = False

    duration_sec = round(time.perf_counter() - tic, 3)

    _record_verify_event(context, db, file, duration_sec, "success", results,
                         bp_check_time=bp_check_time, cid_time=cid_time)

    return Output(results, metadata={
        "duration_sec": duration_sec,
        "file_name": file,
        "bp_check_time_sec": bp_check_time,
        "cid_create_time_sec": cid_time,
        "validated": results.get("validated", False),
        "preview": f"{file} verified in {duration_sec}s",
    })


def _record_verify_event(context, db, file, duration_sec, status, results, **extra):
    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="verify_tape_copy",
            event_type="op_completed",
            status=status,
            metadata={
                "duration_sec": duration_sec,
                "file_name": file,
                "validated": str(results.get("validated", False)),
                **extra,
                "preview": f"{file} verification {status} in {duration_sec}s",
            },
        )
    except Exception:
        pass


def retrieve_json_data(job_id: str) -> Optional[str]:
    """
    Look for matching JSON file
    """
    json_file = [x for x in os.listdir(JSON_PATH) if str(job_id) in str(x)]
    if json_file:
        return os.path.join(JSON_PATH, json_file[0])
    else:
        return None


def check_for_failed_file(file: str, json_file: str) -> bool:
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
        
    notifications = None
    for k, v in dct.items():
        if k == "Notification":
            notifications = v
    if not isinstance(notifications, dict):
        return None
    events = None
    for ky, vl in notifications.items():
        if ky == "Event":
            events = vl
    if not isinstance(events, dict):
        return None
    for key, val in events.items():
        if key == "ObjectsNotPersisted":
            return val
    return None


def create_media_record(file_info: tuple) -> Optional[str]:
    """
    Media record creation for BP ingested file
    """
    
    record_data = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.now())[:10]},
        {"input.time": str(datetime.now())[11:19]},
        {"input.notes": "Digital preservation ingest - automated bulk documentation."},
        {"reference_number": file_info[1]},
        {"imagen.media.original_filename": file_info[1]},
        {"container.file_size.total_bytes": int(file_info[20])},
        {"object.object_number": file_info[16]},
        {"imagen.media.part": file_info[9]},
        {"imagen.media.total": file_info[10]},
        {"preservation_bucket": file_info[18]},
    ]

    media_priref = ""
    print(record_data)
    record_data_xml = adlib.create_record_data(
        CID_API, "media", None, 0, record_data
    )
    print(f"Record data XML: {record_data_xml}")
    try:
        item_rec = adlib.post(CID_API, record_data_xml, "media", "insertrecord")
        print(f"Item record: {item_rec}")
    except Exception as error:
        print(f"\nUnable to create CID media record for {file_info[16]}")
        raise error

    if item_rec:
        try:
            media_priref = adlib.retrieve_field_name(item_rec, "priref")[0]
            print(f"** CID media record created with Priref {media_priref}")
        except Exception as err:
            raise err

    return media_priref
