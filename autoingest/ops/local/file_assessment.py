from ...resources import utils
from ...resources import bp_utils as bp
from ...resources import adlib

import os
import time
import magic
from pathlib import Path
from typing import Optional, Tuple, Union
from dagster import op, Out, Output, OpExecutionContext

CID_API = os.environ.get("CID_API3")


@op(required_resource_keys={"workflow_db"}, config_schema={"file_path": str}, out=Out(dict))
def assess_filename(context: OpExecutionContext) -> Output:
    tic = time.perf_counter()

    file_path = Path(context.op_config["file_path"])
    if not file_path:
        context.log.info("No files found at this time.")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})
    filename = file_path.name
    filetype = file_path.suffix.lower().lstrip(".")
    filesize = file_path.stat().st_size

    db = context.resources.workflow_db
    claimed_id = db.try_claim_file(filename, str(file_path))
    if claimed_id is None:
        context.log.info(f"File {filename} is already being processed. Skipping.")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3), "preview": f"Already processing: {filename}"})

    context.log.info(CID_API)
    field_details = db.lookup_file_details(filename)
    existing_error = ""
    if field_details is None:
        context.log.info(
            f"No field details found for filename '{filename}'"
        )
    else:
        if field_details[4] and len(field_details[4]) > 0:
            print(field_details[4])
            existing_error = field_details[4]
            context.log.warning(f"Historical error found for ingest, will not proceed until error fixed and ingest refreshed: {existing_error}")
            duration_sec = round(time.perf_counter() - tic, 3)
            try:
                db.record_pipeline_event(
                    run_id=context.run_id,
                    job_name=context.job_name,
                    op_name="assess_filename",
                    event_type="op_completed",
                    status="failure",
                    metadata={"duration_sec": duration_sec, "file_name": filename, "preview": f"Historical error: {existing_error}"},
                    message=existing_error,
                )
            except Exception:
                pass
            return Output({}, metadata={"duration_sec": duration_sec, "file_name": filename})

    errors = []
    do_ingest = True
    context.log.info(f"** Assessing file: {filename} ({filetype}, {filesize} bytes)")
    donor, incomplete_scan, screencraft = get_data_from_path(str(file_path))

    filename_check = utils.check_filename(filename, screencraft)
    if not filename_check:
        context.log.info(f"Filename did not pass filename checks: {filename}")
        errors.append("Filename formatted incorrectly")
        do_ingest = False

    if screencraft is True:
        context.log.info("File path identified as Screencraft archive")
        object_number, part, whole = process_image_archive(filename)
    else:
        object_number = utils.get_object_number(filename)
        part, whole = utils.check_part_whole(filename)

    if not part or not whole:
        context.log.info(f"Part whole failed checks: {filename}")
        errors.append(f"Cannot parse partWhole from filename {filename}")
        do_ingest = False
    if not object_number:
        context.log.info(f"Object number could not be extracted from {filename}")
        errors.append("Cannot parse <object_number> from filename")
        do_ingest = False

    if do_ingest and not utils.cid_check(CID_API):
        context.log.info(f"CID API is not responsive — deferring {filename}")
        errors.append("CID API unreachable — will retry on next ingest attempt")
        do_ingest = False

    priref = utils.fetch_item_priref(object_number)
    if not priref:
        context.log.info(f"Cannot find a record with object_number: {object_number}")
        errors.append(f"Cannot find record with <object number> ...<{object_number}>")
        do_ingest = False

    if filesize <= 1099511627776:
        over_tb_accepted = False
    else:
        over_tb_accepted = check_accepted_file_type(str(file_path))

    bucket, bucket_list = bp.get_buckets(donor, over_tb_accepted)
    if not bucket:
        context.log.info(f"Failed to match Donor {donor} to buckets")
        errors.append(f"Failed to match Donor {donor} to Black Pearl bucket")
        do_ingest = False

    mime_type = check_mime_type(str(file_path))
    if mime_type not in ["application", "audio", "image", "video"]:
        context.log.info(f"Mime type does not confirm to accepted type: {mime_type}")
        errors.append(f"MIMEtype '{mime_type}' is not permitted...")
        do_ingest = False

    ffprobe_exit = None
    if mime_type != "application":
        ffprobe_exit = utils.check_ffprobe_exit(str(file_path))
        if ffprobe_exit != 0:
            context.log.info(f"FFprobe failed to read file: {filename} / Exit code: {ffprobe_exit}")
            errors.append(f"FFprobe failed to read file: [{ffprobe_exit}] status")
            do_ingest = False

    ftype = None
    if priref and object_number:
        try:
            file_type_match, ftype = ext_in_file_type(filetype, priref, object_number)
        except Exception as err:
            context.log.warning(f"CID API error during file_type check: {err}")
            file_type_match = False
            ftype = None
            errors.append(f"CID API unreachable during file_type check")
            do_ingest = False
        if not file_type_match:
            context.log.info(f"File exension {filetype} does not match CID Item file_type: {ftype}")
            errors.append(f"Extension '{filetype}' does not match <{ftype}> in record")
            do_ingest = False

    media_check = utils.check_file_has_media_rec(filename)
    if media_check is None:
        context.log.info(f"Media dB could not be reached...")
        errors.append(f"Media dB could not be reached at this time")
        do_ingest = False
    if media_check is True:
        context.log.info(f"Filename already matched to CID media record!")
        errors.append(f"Filename already has a CID Media record: {filename}")
        do_ingest = False
    context.log.info(f"No CID Media record found for file: {filename}")
    """ Temporary restriction for test
    context.log.info(bucket_list)
    context.log.info(filename)
    status = bp.check_no_bp_status(filename, bucket_list)
    context.log.info(status)
    if status is False:
        context.log.info(f"File has already been ingested to Black Pearl: {filename} - Buckets {bucket_list}")
        errors.append(f"Filename has already been ingested to DPI: {filename}")
        do_ingest = False
    """
    if not incomplete_scan or part != 1 or whole != 1:
        previous_part = check_for_multipart(filename, part, whole)
        if previous_part is True:
            pass
        elif previous_part:
            pp_field_details = db.lookup_file_details(previous_part)
            if not pp_field_details:
                context.log.info(f"Skipping ingest - previous part has not been ingested yet")
                errors.append("Skip object as previous part not yet ingested or queued for ingest")
                do_ingest = False
            if pp_field_details[6] == "FALSE":
                context.log.info(f"Skipping ingest - previous part has not been ingested yet")
                errors.append("Skip object as previous part not yet ingested or queued for ingest")
                do_ingest = False

    returns = {}
    returns["file_name"] = filename
    returns["file_path"] = str(file_path)
    returns["file_size"] = filesize
    returns["extension"] = filetype

    if do_ingest is False:
        returns.update({
            "do_ingest": "FALSE",
            "error_message": errors[0],
            "file_status": "Failed assessment"
        })
    else:
        returns.update({
            "do_ingest": "TRUE",
            "error_message": "",
        })
    if incomplete_scan is True:
        returns["incomplete_scan"] = "TRUE"
    else:
        returns["incomplete_scan"] = "FALSE"
    if screencraft is True:
        returns["screencraft_arch"] = "TRUE"
    else:
        returns["screencraft_arch"] = "FALSE"

    if filesize > 1099511627776:
        returns["put_type"] = "Blob"
        autoingest_path = f"autoingest/processing/{donor.lower()}/blobbed/"
    else:
        returns["put_type"] = "Group"
        autoingest_path = f"autoingest/processing/{donor.lower()}/"
    context.log.info(autoingest_path)

    if do_ingest:
        returns["file_status"] = "assessed"
    returns["part"] = part
    returns["whole"] = whole
    returns["ffprobe_exit"] = ffprobe_exit
    returns["bp_bucket"] = bucket
    returns["bucket_list"] = bucket_list
    returns["mime_type"] = mime_type
    returns["cid_file_type"] = ftype
    returns["cid_item_priref"] = priref
    returns["cid_ob_num"] = object_number
    returns["source"] = donor
    returns["autoingest_path"] = autoingest_path

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app.file_catalogue
                SET file_status = %s,
                    do_ingest = %s,
                    error_message = %s,
                    file_size = %s,
                    mime_type = %s,
                    source = %s,
                    extension = %s,
                    part = %s,
                    whole = %s,
                    ffprobe_exit = %s,
                    bp_bucket = %s,
                    bucket_list = %s,
                    cid_file_type = %s,
                    cid_item_priref = %s,
                    cid_ob_num = %s,
                    incomplete_scan = %s,
                    screencraft_arch = %s,
                    put_type = %s,
                    autoingest_path = %s,
                    file_path = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (
                returns.get("file_status", "Failed assessment"),
                returns.get("do_ingest", "FALSE"),
                returns.get("error_message", ""),
                returns.get("file_size", 0),
                returns.get("mime_type", ""),
                returns.get("source", ""),
                returns.get("extension", ""),
                returns.get("part"),
                returns.get("whole"),
                returns.get("ffprobe_exit"),
                returns.get("bp_bucket", ""),
                str(returns.get("bucket_list", "")),
                returns.get("cid_file_type", ""),
                returns.get("cid_item_priref", ""),
                returns.get("cid_ob_num", ""),
                returns.get("incomplete_scan", "UNKNOWN"),
                returns.get("screencraft_arch", "UNKNOWN"),
                returns.get("put_type", ""),
                returns.get("autoingest_path", ""),
                returns.get("file_path", str(file_path)),
                claimed_id,
            ))

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)
    status_outcome = "success" if do_ingest else "failure"

    metadata = {
        "duration_sec": duration_sec,
        "file_name": filename,
        "file_size": filesize,
        "mime_type": mime_type,
        "source": donor,
        "do_ingest": "TRUE" if do_ingest else "FALSE",
        "preview": f"{filename} ({filesize} bytes, {mime_type}, {donor}) assessed in {duration_sec}s",
    }

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="assess_filename",
            event_type="op_completed",
            status=status_outcome,
            metadata=metadata,
            message=errors[0] if errors else None,
        )
    except Exception:
        pass

    return Output(returns, metadata=metadata)


def get_data_from_path(fpath: str) -> Tuple[str, bool, bool]:
    source = ""
    solo_reel = False
    screencraft = False

    if "/ingest/netflix/" in fpath:
        source = "Netflix"
    elif "/ingest/amazon/" in fpath:
        source = "Amazon"
    elif "/ingest/disney/" in fpath:
        source = "Disney"
    else:
        source = "BFI"
    if "/incomplete_scan/" in fpath:
        solo_reel = True
    if "/Screencraft/" in fpath and "/proxy/image/archive/" in fpath:
        screencraft = True

    return source, solo_reel, screencraft


def check_accepted_file_type(fpath: str) -> bool:
    formt: str = utils.get_metadata("Video", "Format", fpath)

    if any(x in fpath for x in ["qnap_11", "qnap_10"]):
        if fpath.endswith((".tar", ".TAR", ".mkv", ".MKV")):
            return True
        elif "ProRes" in str(formt):
            return True
    if any(x in fpath for x in ["qnap_06", "qnap_03", "qnap_07"]):
        if fpath.endswith((".mkv", ".MKV", ".tar", ".TAR")):
            return True
        elif "ProRes" in str(formt):
            return True
    if any(x in fpath for x in ["bp_nas/film", "EditShare-Director", "bp_nas/digital"]):
        if fpath.endswith((".tar", ".TAR", ".mkv", ".MKV")):
            return True
        elif "ProRes" in str(formt):
            return True

    return False


def check_mime_type(fpath: str) -> str:
    mime = utils.sort_ext(fpath)
    if not mime:
        mime = magic.from_file(fpath, mime=True)
        if "/" in mime:
            mime = mime.split("/")[0]

    return mime


def process_image_archive(
    fname: str
) -> tuple[Optional[str], Optional[int], Optional[int]]:

    split_name = fname.split("_")
    object_number, part, whole = "", "", ""
    if "-" in fname:
        print("* Cannot parse <object_number> from filename...")
        return None, None, None
    try:
        object_number = "-".join(split_name[:-1])
    except Exception:
        print("* Cannot parse <object_number> from filename...")
        return None, None, None
    try:
        partwhole = split_name[-1].split(".")[0]
        part, whole = partwhole.split("of")
        if len(part) != len(whole):
            return None, None, None
        if len(part) > 4:
            return None, None, None
        if int(part) == 0:
            return None, None, None
        if int(part) > int(whole):
            return None, None, None
    except Exception as err:
        return None, None, None

    return object_number, int(part), int(whole)


def ext_in_file_type(
    ext: str, priref: str, ob_num: str
) -> Tuple[bool, Optional[str]]:

    ftype = utils.accepted_file_type(ext)
    if not ftype:
        return False, None

    ftype_list = ftype.split(", ")

    if ob_num.startswith("CA-"):
        retrieved_fields = ["asset_file_type"]
    else:
        retrieved_fields = ["file_type"]

    search = f"priref={priref}"
    record = adlib.retrieve_record(
        CID_API, "collect", search, "1", retrieved_fields
    )[1]
    if record is None:
        return False, None

    try:
        file_type = adlib.retrieve_field_name(record[0], retrieved_fields[0])
        if file_type is None or file_type[0] is None:
            return False, None
    except (IndexError, KeyError):
        return False, None

    if len(file_type) == 1:
        for ft in ftype_list:
            ft = ft.strip()
            if file_type[0] is None:
                return False, None
            if ft == file_type[0].lower():
                return True, file_type[0]
            elif ft == "mxf" and ft in file_type[0].lower():
                return True, file_type[0]
        return False, file_type
    else:
        return False, file_type


def check_for_multipart(filename: str, part: int | None, whole: int | None) -> Union[bool, str]:

    if part is None or whole is None:
        return True

    file_split = filename.split("_")
    if len(file_split) == 4:
        file = "_".join(file_split[:3])
    else:
        file = "_".join(file_split[:-1])

    if whole == 1:
        return True
    elif part == 1:
        return True

    str_part = filename.split("_")[-1].split(".")[0].split("of")[0]
    fill_num = len(str_part)

    filename_range = []
    range_whole = whole + 1
    for num in range(1, range_whole):
        filename_range.append(f"{file}_{str(num).zfill(fill_num)}of{str(whole).zfill(fill_num)}")

    previous = part - 2
    previous_part = filename_range[previous]

    return previous_part
