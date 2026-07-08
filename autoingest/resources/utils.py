"""
Repeated utilities
useful for autoingest
workflow, across ops.
"""

import os
import re
import yaml
import json
import shutil
import xxhash
import hashlib
import subprocess
from typing import Final, Optional, Union, List, Tuple
import ffmpeg

# BFI library
import autoingest.resources.adlib as adlib

STORAGE_JSON: str = os.path.join(os.environ.get("LOG_PATH"), "storage_control.json")
CONTROL_JSON: str = os.path.join(os.environ.get("LOG_PATH"), "downtime_control.json")
PREFIX = ["N", "C", "PD", "SPD", "PBS", "PBM", "PBL", "SCR", "CA"]
DPI_BUCKETS = os.environ.get("DPI_BUCKET")
CID_API = os.environ.get("CID_API3")

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


def accepted_file_type(ext: str) -> Optional[str]:
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


def read_yaml(file: str) -> dict:
    """
    Safe open yaml and return as dict
    """
    with open(file) as config_file:
        d = yaml.safe_load(config_file)
        return d


def check_storage(filepath: str) -> Union[bool, str]:
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


def check_control(arg: str) -> bool:
    """
    Check control json for downtime requests
    based on passed argument
    if not utils.check_control['arg']:
        sys.exit(message)
    """
    if not isinstance(arg, str):
        arg = str(arg)

    with open(CONTROL_JSON) as control:
        j: dict[str, str] = json.load(control)
        if j[arg]:
            return True
        else:
            return False


def cid_check(cid_api: Optional[str]) -> Optional[bool]:
    """
    Tests if CID API is operational before
    any other adlib-dependent operations run.
    Returns False if unreachable, True if healthy.
    """
    if cid_api is None:
        return False
    try:
        dct = adlib.check(cid_api)
        if isinstance(dct, dict):
            return True
        return False
    except Exception as err:
        print(f"CID API health check failed: {err}")
        return False


def check_filename(fname: str, screencraft: bool) -> bool:
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


def get_size(fpath: str) -> Union[bool, int]:
    """
    Check the size of given folder path
    return size in kb
    """
    if os.path.isfile(fpath):
        return os.path.getsize(fpath)

    try:
        byte_size: int = sum(
            os.path.getsize(os.path.join(fpath, f))
            for f in os.listdir(fpath)
            if os.path.isfile(os.path.join(fpath, f))
        )
        return byte_size
    except OSError as err:
        print(f"get_size(): Cannot reach folderpath for size check: {fpath}\n{err}")
        return None


def get_metadata(stream: str, arg: str, dpath: str) -> str:
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


def probe_metadata(arg: str, stream: str, fpath: str) -> Optional[str]:
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


def check_part_whole(fname: str) -> Tuple[Optional[int], Optional[int]]:
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


def get_object_number(fname: str) -> Optional[str]:
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


def sort_ext(ext: str) -> Optional[str]:
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
        "application": [
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
            "ttml",
        ],
    }

    ext = ext.lower()
    for key, val in mime_type.items():
        if str(ext) in str(val):
            return key


def exif_data(dpath: str) -> Optional[list[str]]:
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


def check_ffprobe_exit(fpath: str) -> Union[int, bool]:
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


def get_mediaconch(dpath: str, policy: str) -> bool:
    """
    Check for 'pass! {path}' in mediaconch reponse
    for supplied file path and policy
    """

    cmd = ["mediaconch", "--force", "-p", policy, dpath]

    meta = subprocess.check_output(cmd).decode("utf-8")
    return meta.startswith(f"pass! {dpath}")


def get_duration(filepath: str) -> Optional[str]:
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


def create_md5_65536(fpath: str) -> Optional[str]:
    """
    Hashlib MD5 generation using file_digest, return as 32 character hexdigest
    """
    try:
        return hashlib.file_digest(open(fpath, "rb"), "md5").hexdigest()
    except Exception as err:
        print(f"{fpath} - Unable to generate MD5 checksum")
        print(err)
        return None


def create_xxhash_65536(fpath: str) -> Optional[str]:
    """
    Get XXHash checksum for use later
    """
    try:
        x = xxhash.xxh32()
        with open(fpath, 'rb') as fname:
            for chunk in iter(lambda: fname.read(4_194_304), b""):
                x.update(chunk)
        return x.hexdigest()

    except Exception as err:
        print(f"{fpath} - Unable to generate MD5 checksum")
        print(err)
        return None


def fetch_item_priref(ob_num: str) -> str:
    """
    Retrieve item priref, title from CID
    """
    ob_num = ob_num.strip()
    search = f"object_number='{ob_num}'"
    print(f"Search used against CID Collect dB: {search}")
    try:
        record = adlib.retrieve_record(CID_API, "collect", search, "1")[1]
    except Exception as err:
        print(f"fetch_item_priref(): CID API unreachable — {err}")
        return ""
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
    fname: str
) -> bool | None:
    """
    Check if CID media record
    already created for filename
    """
    search = f"imagen.media.original_filename='{fname}'"
    print(f"Search used against CID Media dB: {search}")
    try:
        hits = adlib.retrieve_record(CID_API, "media", search, "0")[0]
    except Exception as err:
        print(f"Unable to retrieve CID Media record {err}")
        return None

    if hits is None:
        print(f"CID API was unreachable for Media search: {search}")
        return None

    print(f"check_media_record(): AdlibV3 record for hits: {hits}")
    if int(hits) >= 1:
        return True
    else:
        return False



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
    elif arg == "mdata_full_json":
        data = mediainfo_create("-f", "JSON", fpath)

    return data.decode("utf-8").strip()


def mediainfo_create(arg: str, output_type: str, filepath: str, mediainfo_path: Optional[str] = None) -> Optional[bytes]:
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
        results = subprocess.run(command, shell=False, capture_output=True)
    except Exception as err:
        print(err)
        return None

    # Check file created has contents
    if results.stdout:
        return results.stdout
    if results.stderr:
        return results.stderr


def cid_media_append(priref: str, data: list[str]) -> Optional[bool]:
    """
    Receive data and priref and append to CID media record
    """
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_mid = "".join(data)
    payload_end = f"</record></recordList></adlibXML>"
    payload = payload_head + payload_mid + payload_end

    try:
        rec = adlib.post(CID_API, payload, "media", "updaterecord")
    except Exception as err:
        print(f"cid_media_append(): CID API unreachable — {err}")
        return False
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


def move_file(from_path: str, to_path: str) -> Optional[List[Union[bool, str]]]:
    """
    Shutil move function reporting of success/failure
    """
    if not os.path.isfile(from_path):
        return None

    try:
        shutil.move(from_path, to_path)
        print(f"Moved first path to second path:\n{from_path}\n{to_path}")
    except Exception as err:
        print(f"General error for move: {err}")

    if os.path.isfile(from_path):
        return [False, f"FAIL: File did not move to {from_path}"]
    if os.path.isfile(to_path):
        return [True, f"SUCCESS: File moved to new path {to_path}"]
