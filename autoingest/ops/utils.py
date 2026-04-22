"""
Repeated utilities
useful for autoingest
workflow, across ops.
"""

import os
import re
import json
import xxhash
import hashlib
import subprocess
from requests import Session
from typing import Final, Optional
import ffmpeg

# BFI library
import adlib

CONTROL_JSON: str = os.path.join(os.environ.get("LOG_PATH"), "downtime_control.json")
STORAGE_JSON: str = os.path.join(os.environ.get("LOG_PATH"), "storage_control.json")
PREFIX = ["N", "C", "PD", "SPD", "PBS", "PBM", "PBL", "SCR", "CA"]
DPI_BUCKETS = os.environ.get("DPI_BUCKET")

ACCEPTED_EXT: Final = [
    "avi",
    "mxf",
    "xml",
    "tar",
    "dpx",
    "wav",
    "mpg",
    "mpeg",
    "mp4",
    "m2ts",
    "mov",
    "mkv",
    "wmv",
    "tif",
    "tiff",
    "jpg",
    "jpeg",
    "ts",
    "m2ts",
    "rtf",
    "ttf",
    "srt",
    "scc",
    "itt",
    "stl",
    "cap",
    "dfxp",
    "dxfp",
    "csv",
    "pdf",
    "txt",
    "vtt",
    "ttml",
]


def accepted_file_type(ext):
    """
    Receive extension and returnc
    matching accepted file_type
    """
    ftype = {
        "avi": "avi",
        "imp": "mxf, xml",
        "tar": "dpx, dcp, dcdm, wav",
        "mxf": "mxf, 50i, imp",
        "mpg": "mpeg-1, mpeg-ps",
        "mpeg": "mpeg-1, mpeg-ps",
        "mp4": "mp4",
        "mov": "mov, prores",
        "mkv": "mkv, dpx, dcdm",
        "wav": "wav",
        "wmv": "wmv",
        "tif": "tif, tiff",
        "tiff": "tif, tiff",
        "jpg": "jpg, jpeg",
        "jpeg": "jpg, jpeg",
        "ts": "mpeg-ts",
        "m2ts": "mpeg-ts",
        "srt": "srt",
        "xml": "xml, imp",
        "scc": "scc",
        "itt": "itt",
        "stl": "stl",
        "rtf": "rtf",
        "ttf": "ttf",
        "vtt": "vtt",
        "cap": "cap",
        "dxfp": "dxfp",
        "dfxp": "dfxp",
        "csv": "csv",
        "pdf": "pdf",
        "txt": "txt",
        "ttml": "ttml",
    }

    ext = ext.lower()
    for key, val in ftype.items():
        if key == ext:
            return val

    return None


def check_storage(filepath):
    """
    check if storage is avaliable for use
    Returns bool, or string
    """
    with open(STORAGE_JSON, "r") as storage:
        storage_dict: dict[str, str] = json.load(storage)

    if not storage_dict["all_storage_on"]:
        return False

    for key in storage_dict.keys():
        if filepath.startswith(key):
            return storage_dict[key]

    return "Storage not found"


def cid_check(cid_api):
    """
    Tests if CID API operational before
    all other operations commence
    if not utils.cid_check[API]:
        sys.exit(message)
    """
    if cid_api is None:
        return False
    try:
        dct = adlib.check(cid_api)
        print(dct)
        if isinstance(dct, dict):
            return True
    except KeyError:
        return False


def check_filename(fname, screencraft):
    """
    Run series of checks against BFI filenames
    check accepted prefixes, and extensions
    """
    if not screencraft:
        if not any(fname.startswith(px) for px in PREFIX):
            return False
    if not re.search("^[A-Za-z0-9_.]*$", fname):
        return False

    sname: list[str] = fname.split("_")
    if len(sname) > 4 or len(sname) < 3:
        return False
    if len(sname) == 4 and len(sname[2]) != 1:
        return False

    if "." in fname:
        if len(fname.split(".")) != 2:
            return False
        ext = fname.split(".")[-1]
        if ext.lower() not in ACCEPTED_EXT:
            return False

    return True


def get_metadata(stream, arg, dpath):
    """
    Retrieve metadata with subprocess
    for supplied stream/field arg
    """

    cmd: list[str] = [
        "mediainfo",
        "--Full",
        "--Language=raw",
        f"--Output={stream};%{arg}%",
        dpath,
    ]

    meta = subprocess.check_output(cmd)
    return meta.decode("utf-8").strip()


def probe_metadata(arg, stream, fpath):
    """
    Use FFmpeg module to extract
    ffprobe data from file
    """
    if arg == "duration":
        new_args = "DURATION"
    else:
        new_args = arg
    try:
        probe = ffmpeg.probe(fpath)
        for i in probe["streams"]:
            if i["codec_type"] == stream and new_args == "DURATION":
                return i["tags"][new_args]
            return i[new_args]

    except ffmpeg.Error as err:
        print(err)
        return None


def check_part_whole(fname):
    """
    Check part whole well formed
    """
    match: Optional[re.Match[str]] = re.search(r"(?:_)(\d{2,4}of\d{2,4})(?:\.)", fname)
    if not match:
        print("* Part-whole has illegal charcters...")
        return None, None
    part, whole = [int(i) for i in match.group(1).split("of")]
    len_check = fname.split("_")[-1].split(".")[0]
    str_part, str_whole = len_check.split("of")
    if len(str_part) != len(str_whole):
        return None, None
    if part > whole:
        print("* Part is larger than whole...")
        return None, None
    return part, whole


def get_object_number(fname):
    """
    Extract object number from name formatted
    with partWhole, eg GUR_123456_01of01.ext
    """

    try:
        splits: list[str] = fname.split("_")
        object_number: Optional[str] = "-".join(splits[:-1])
    except Exception:
        object_number = None
    return object_number


def sort_ext(ext):
    """
    Decide on file type
    """
    mime_type = {
        "video": [
            "mxf",
            "mkv",
            "mov",
            "wmv",
            "mp4",
            "mpg",
            "avi",
            "ts",
            "mpeg",
            "m2ts",
        ],
        "image": ["png", "gif", "jpeg", "jpg", "tif", "pct", "tiff"],
        "audio": ["wav", "flac", "mp3"],
        "document": [
            "docx",
            "pdf",
            "vtt",
            "doc",
            "tar",
            "srt",
            "scc",
            "itt",
            "stl",
            "cap",
            "dxfp",
            "xml",
            "dfxp",
            "txt",
            "ttf",
            "rtf",
            "csv",
            "txt",
            "ttml",
        ],
    }

    ext = ext.lower()
    for key, val in mime_type.items():
        if str(ext) in str(val):
            return key


def exif_data(dpath):
    """
    Retrieve exiftool data
    return match to field if available
    """

    cmd = ["exiftool", dpath]
    try:
        data = subprocess.run(cmd, shell=False, capture_output=True)
        data = data.stdout.decode("latin-1")
        print(data)
    except subprocess.CalledProcessError as err:
        print(err)
        return None
    exif_d = data.split("exiftool', '")[-1]
    exif_md = exif_d.split("']")[0]
    exif_metadata = exif_md.split("\n")
    return exif_metadata


def check_ffprobe_exit(fpath):
    """
    Get return code for read attempt
    """
    cmd = ["ffprobe", "-i", fpath, "-loglevel", "-8"]
    try:
        code = subprocess.run(cmd, check=True, shell=False)
        print(f"*ffprobe read file successfully - status {code.returncode}")
        return code.returncode
    except Exception as err:
        print(err)
    return False


def get_mediaconch(dpath, policy):
    """
    Check for 'pass! {path}' in mediaconch reponse
    for supplied file path and policy
    """

    cmd = ["mediaconch", "--force", "-p", policy, dpath]

    meta = subprocess.check_output(cmd).decode("utf-8")
    if meta.startswith(f"pass! {dpath}"):
        return True, meta

    return False, meta


def get_duration(filepath):
    """
    Retrieve duration field if possible
    """
    retry = False
    duration = ""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        "-sexagesimal",
        filepath,
    ]
    try:
        duration = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as err:
        print(f"Unable to extract duration with FFprobe: {err}")
        retry = True

    if retry:
        cmd = [
            "mediainfo",
            "--Language=raw",
            "-f",
            "--Output=General;%Duration/String3%",
            filepath,
        ]

        try:
            duration = subprocess.check_output(cmd)
        except Exception as err:
            print(f"Unable to extract duration with MediaInfo: {err}")
    if duration:
        return duration.decode("utf-8").rstrip("\n")
    return None


def create_md5_65536(fpath):
    """
    Hashlib md5 generation, return as 32 character hexdigest
    """
    try:
        hash_md5 = hashlib.md5()
        with open(fpath, "rb") as fname:
            for chunk in iter(lambda: fname.read(65536), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    except Exception as err:
        print(f"{fpath} - Unable to generate MD5 checksum")
        print(err)
        return None


def create_xxhash_65536(fpath):
    """
    Get XXHash checksum for use later
    """
    try:
        x = xxhash.xxh32()
        with open(fpath, 'rb') as fname:
            for chunk in iter(lambda: fname.read(65536), b""):
                x.update(chunk)
        return x.hexdigest()

    except Exception as err:
        print(f"{fpath} - Unable to generate MD5 checksum")
        print(err)
        return None


def get_current_api():
    """
    Check control json for downtime requests
    based on passed argument
    if not utils.check_control['arg']:
        sys.exit(message)
    """

    try:
        with open(CONTROL_JSON) as control:
            j: dict[str, str] = json.load(control)
            if j["current_api"]:
                api_key = j["current_api"]
                return os.environ.get(api_key)
            else:
                print("No API key found in control json")
                return None
    except FileNotFoundError:
        print(f"Control JSON file not found: {CONTROL_JSON}")
        return None


def fetch_item_priref(ob_num: str) -> str:
    """
    Retrieve item priref, title from CID
    """
    ob_num = ob_num.strip()
    search = f"object_number='{ob_num}'"
    print(f"Search used against CID Collect dB: {search}")
    record = adlib.retrieve_record(CID_API, "collect", search, "1")[1]
    print(f"get_item_priref(): AdlibV3 record for priref:\n{record}")

    if record is None:
        return ""
    try:
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
        print(f"get_item_priref(): AdlibV3 priref: {priref}")
    except Exception:
        priref = ""

    return priref or ""


def check_file_has_media_rec(
    fname: str, session: Session
) -> Optional[Union[str, bool]]:
    """
    Check if CID media record
    already created for filename
    """
    search = f"imagen.media.original_filename='{fname}'"
    print(f"Search used against CID Media dB: {search}")
    try:
        hits = adlib.retrieve_record(CID_API, "media", search, "0", session)[0]
    except Exception as err:
        print(f"Unable to retrieve CID Media record {err}")
        return None

    if hits is None:
        print(f"CID API was unreachable for Media search: {search}")
        raise Exception(f"CID API was unreachable for Media search: {search}")

    print(f"check_media_record(): AdlibV3 record for hits: {hits}")
    if int(hits) == 1:
        return True
    elif int(hits) == 0:
        return False
    if int(hits) > 1:
        return f"Hits exceed 1: {hits}"


def make_metadata(fpath: str, arg: str) -> str:
    """
    Create mediainfo files
    Check before each run that file is still
    in same path, otherwise search in local
    black_pearl_ingest path for new path
    """

    if arg == "mdata_full_text":
        data = mediainfo_create("-f", "TEXT", fpath)
    elif arg == "mdata_text":
        data = mediainfo_create("", "TEXT", fpath)
    elif arg == "mdata_ebucore":
        data = mediainfo_create("", "EBUCore", fpath)
    elif arg == "mdata_pbcore":
        data = mediainfo_create("", "PBCore2", fpath)
    elif arg == "mdata_full_xml":
        data = mediainfo_create("-f", "XML", fpath)
    elif arg == "mdata_full_js0n":
        data = mediainfo_create("-f", "JSON", fpath)

    return data.decode("utf-8").strip()


def mediainfo_create(arg, output_type, filepath, mediainfo_path):
    """
    Output mediainfo data to text files
    """

    command: list[str] = [
        "mediainfo",
        arg,
        f"--Output={output_type}",
        filepath,
    ]

    try:
        results = subprocess.run(command, shell=False, check_output=True)
    except Exception as err:
        print(err)
        return None

    # Check file created has contents
    if results.stdout:
        return results.stdout
    if results.stderr:
        return results.stderr


def get_media_input_date(filename: str) -> str:
    """
    Call up adlib to get input.date field
    """
    api = get_current_api()
    try:
        rec = adlib.retrieve_record(api, "media", f"reference_number='{filename}'", "1")[1]
        input_date = adlib.retrieve_field_name(rec[0], "input.date")[0]
        if len(input_date) == 10:
            return "".join(input_date.split("-")[:2])
    except Exception as err:
        print(err)


def get_current_api():
    """
    Check control json for downtime requests
    based on passed argument
    if not utils.check_control['arg']:
        sys.exit(message)
    """

    try:
        with open(CONTROL_JSON) as control:
            j: dict[str, str] = json.load(control)
            if j["current_api"]:
                api_key = j["current_api"]
                return os.environ.get(api_key)
            else:
                print("No API key found in control json")
                return None
    except FileNotFoundError:
        print(f"Control JSON file not found: {CONTROL_JSON}")
        return None


def cid_media_append(priref: str, data: list[str]) -> Optional[bool]:
    """
    Receive data and priref and append to CID media record
    """
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_mid = "".join(data)
    payload_end = f"</record></recordList></adlibXML>"
    payload = payload_head + payload_mid + payload_end

    rec = adlib.post(CID_API, payload, "media", "updaterecord")
    if rec is None:
        return False

    if "access_rendition" in str(rec):
        return True


def get_buckets(bucket_collection: str, blob_accepted: bool) -> tuple[str, list[str]]:
    """
    Read JSON list return
    key_value and list of others
    """
    bucket_list: list[str] = []
    key_bucket: str = ""

    with open(DPI_BUCKETS) as data:
        bucket_data: dict[str, str] = json.load(data)
    for key, value in bucket_data.items():
        if bucket_collection.lower() == "bfi":
            if blob_accepted:
                if "preservationblobbing0" in str(key.lower()):
                    if value is True:
                        key_bucket = key
                    bucket_list.append(key)
            else:
                if "preservationblobbing0" in str(key.lower()):
                    continue
                if "preservation0" in str(key.lower()):
                    if value is True:
                        key_bucket = key
                    bucket_list.append(key)
                elif "imagen" in str(key):
                    bucket_list.append(key)
        elif bucket_collection.lower() in ("netflix", "amazon"):
            if blob_accepted:
                if f"{bucket_collection.lower()}blobbing" in key:
                    if value is True:
                        key_bucket = key
                    bucket_list.append(key)
            else:
                if f"{bucket_collection.strip()}blobbing" in key:
                    continue
                elif f"{bucket_collection.strip()}0" in key:
                    if value is True:
                        key_bucket = key
                    bucket_list.append(key)

    return key_bucket, bucket_list


def get_buckets_blob(bucket_collection: str) -> str:
    """
    Read JSON list return
    key_value and list of others
    """
    key_bucket: str = ""

    with open(DPI_BUCKETS) as data:
        bucket_data: dict[str, str] = json.load(data)
    if bucket_collection == "netflix":
        for key, value in bucket_data.items():
            if "netflixblobbing" in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == "amazon":
        for key, value in bucket_data.items():
            if "amazonblobbing" in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == "bfi":
        for key, value in bucket_data.items():
            if "preservationblobbing" in key.lower():
                if value is True:
                    key_bucket = key

    return key_bucket
