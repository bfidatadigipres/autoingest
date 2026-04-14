"""
Mostly completed, for review later
- Creates blocks of metadata text and saves into file_info dB
- Creates checksums and saves into enriched_file_info dB
"""

import json
import utils
from datetime import datetime
from dagster import op, Out


@op(
    out={"enriched_file_info": Out(dict)},
    tags={"dagster-celery/queue": "default"},
)
def extract_metadata(context, file_info: dict) -> dict:
    file_path = file_info["file_path"]
    context.log.info(f"** Extracting metadata from {file_path}")

    mdata_type = [
        "mdata_full_text",
        "mdata_text",
        "mdata_ebucore",
        "mdata_pbcore",
        "mdata_full_xml",
        "mdata_full_json"
    ]

    for mtype in mdata_type:
        mdata = utils.make_metadata(file_path, mtype)
        if "json" in mdata:
            metadata = json.dumps(mdata)
            file_info[mtype] = metadata        
        else:
            file_info[mtype] = mdata

    return file_info


@op(
    out={"checksummed_file_info": Out(dict)},
    tags={"dagster-celery/queue": "default"},
)
def generate_checksum(context, enriched_file_info: dict) -> dict:
    file_path = enriched_file_info["file_path"]
    context.log.info(f"Generating MD5 checksum for {file_path}")

    md5 = utils.create_md5_65536(file_path)
    enriched_file_info["checksum_md5"] = md5
    xxhash = utils.create_xxhash_66536(file_type)
    enriched_file_info["checksum_xxh"] = xxhash
    enriched_file_info["checksum_date"] = str(datetime.now())[:19]
            
    context.log.info(f"Checksum MD5: {md5} / Checksum XXHash: {xxhash}")
    return enriched_file_info

