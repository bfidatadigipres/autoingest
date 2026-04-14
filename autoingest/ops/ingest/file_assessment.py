import os
import utils
import bp_utils as bp
import adlib
import magic
from pathlib import Path
from typing import Optional
from dagster import op, Config, Out, Output


class FileAssessmentConfig(Config):
    file_path: str


@op(
    out={"file_info": Out(dict)},
    tags={"dagster-celery/queue": "default"},
)
def assess_filename(context, config: FileAssessmentConfig, workflow_db) -> dict:
    file_path = Path(config.file_path)
    filename = file_path.name
    filetype = file_path.suffix.lower().lstrip(".")
    filesize = file_path.stat().st_size

    field_details = workflow_db.lookup_file_details(filename)
    if field_details is None:
        context.log.warning(
            f"No field details found for filename '{filename}', using defaults"
        )
    else:
        if len(field_details[4]) > 0:
            print(field_details[4])
            existing_error = field_details.get("error_message")

    # Check file ready for ingest
    errors = []
    process = True
    context.log.info(f"** Assessing file: {filename} ({filetype}, {filesize} bytes)")
    donor, incomplete_scan, screencraft = get_data_from_path(file_path)

    filename_check = utils.check_filename(filename, screencraft)
    if not filename_check:
        errors.append("Filename formatted incorrectly")
        process = False

    if screencraft is True:
        object_number, part, whole = process_image_archive(filename)
    else:
        object_number = utils.get_object_number(filename)
        part, whole = utils.check_part_whole(filename)

    if not part or not whole:
        errors.append(f"Cannot parse partWhole from filename {filename}")
        process = False
    if not object_number:
        errors.append("Cannot parse <object_number> from filename")
        process = False

    priref = utils.fetch_item_priref(object_number)
    if not priref:
        error.append(f"Cannot find record with <object number> ...<{object_number}>")
        process = False

    over_tb_accepted = check_accepted_file_type(file_path)
    bucket, bucket_list = bp.get_buckets(donor, over_tb_accepted)
    if not bucket:
        error = ""
        error = ""
        process = False

    mime_type = check_mime_type(file_path)
    if mime_type not in ["application", "audio", "image", "video"]:
        errors.append(f"MIMEtype '{mime_type}' is not permitted...")
        process = False

    ffprobe_exit = utils.check_ffprobe_exit(file_path)
    if ffprobe_exit != 0:
        errors.append(f"FFprobe failed to read file: [{ffprobe_exit}] status")
        process = False

    if priref and object_number:
        file_type_match, ftype = ext_in_file_type(filetype, priref, object_number)
        if not file_type_match:
            errors.append(f"Extension '{filetype}' does not match <{ftype}> in record")
            process = False

    media_check = utils.check_file_has_media_rec(filename)
    if media_check is None:
        errors.append(f"Media dB could not be reached at this time")
        process = False
    if media_check is True:
        errors.append(f"Filename already has a CID Media record: {filename}")
        process = False
    elif "Hits exceed 1" in media_check:
        errors.append(f"Filename {filename} has more than one CID Media record. Manual attention needed.")

    status = bp.check_no_bp_status(filename, bucket_list)
    if status is False:
        errors.append(f"Filename has aleady been ingested to DPI: {filename}")
        process = False
    
    if not incomplete_scan or part != 1 or whole != 1:
        # pervious part returns without extension, dB search uses LIKE to match most of name
        previous_part = check_for_multipart(filename, part, whole)
        pp_field_details = workflow_db.lookup_file_details(previous_part)
        if not pp_field_details:
            errors.append("Skip object as previous part not yet ingested or queued for ingest")
            process = False
        if pp_field_details.get('do_ingest') == False:
            errors.append("Skip object as previous part not yet ingested or queued for ingest")
            process = False

    if existing_error in errors:
        return {}
    if process is False:
        returns = {
            "do_ingest": "False",
            "error_message": errors[0],
        }
    else:
        returns = {
            "do_ingest": "True",
            "error_message": "",
        }

    returns.extend({
        "file_path": str(file_path),
        "file_status": "File assessment complete",
        "file_name": filename,
        "file_size": filesize,
        "extension": ext,
        "part": part,
        "whole": whole,
        "ffprobe_exit": ffprobe_exit,
        "bp_bucket": bucket,
        "bucket_list": bucket_list,
        "mime_type": mime_type,
        "cid_file_type": ftype,
        "cid_item_priref": priref,
        "cid_ob_num": object_number,
        "source": donor,
        "incomplete_scan": incomplete_scan,
        "screencraft_archive": screencraft,
    })
    return returns


def get_data_from_path(fpath):
    """
    Get the source ingest path
    """
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
    """
    Retrieve codec and ensure file is accepted type
    TAR accepted from DMS / ProRes all other paths
    """
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


def check_mime_type(fpath: str) -> bool:
    """
    Checks the mime type of the file
    and if stream media checks ffprobe
    """
    if fpath.lower().endswith((".mxf", ".ts", ".mpg", ".m2ts")):
        mime = "video"
    elif fpath.lower().endswith(
        (
            ".csv",
            ".pdf",
            ".srt",
            ".rtf",
            ".scc",
            ".xml",
            ".itt",
            ".stl",
            ".cap",
            ".dfxp",
            ".dxfp",
            ".vtt",
            ".ttml",
            ".ttf",
            ".txt",
        )
    ):
        mime = "application"
    else:
        mime = magic.from_file(fpath, mime=True)
    if "/" in mime:
        mime = mime.split("/")[0]

    return mime


def process_image_archive(
    fname: str
) -> tuple[Optional[str], Optional[int], Optional[int]]:
    """
    Process special collections image
    archive filename structure
    """

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
) -> Optional[bool]:
    """
    Check if ext matches file_type
    """

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
        CID_API, "collect", search, "1", session, retrieved_fields
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


def check_for_multipart(filename: str, part: int, whole: int):
    """
    Get previous part and check if already in dB
    """
    if whole == 1:
        return True
    elif part == 1:
        return True

    str_part, str_whole = filename.split("_")[-1].split(".")[0].split("of")
    fill_num = len(str_part)

    filename_range = []
    range_whole = whole + 1
    for num in range(1, range_whole):
        filename_range.append(f"{filename}_{str(num).zfill(fill_num)}of{str(whole).zfill(fill_num)}")
 
    previous = part - 2
    previous_part = filename_range[previous]
    