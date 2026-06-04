"""
Mostly completed, for review later
- Creates blocks of metadata text and saves into file_info dB
- Creates checksums and saves into enriched_file_info dB
"""

import json
import autoingest.resources.utils as utils
from datetime import datetime
from dagster import op, OpExecutionContext


@op
def extract_metadata(context: OpExecutionContext, file_info: dict) -> dict:
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


@op
def generate_checksum(context: OpExecutionContext, enriched_file_info: dict) -> dict:
    file_path = enriched_file_info["file_path"]
    context.log.info(f"Generating MD5 checksum for {file_path}")

    md5 = utils.create_md5_65536(file_path)
    enriched_file_info["checksum_md5"] = md5
    xxhash = utils.create_xxhash_66536(file_path)
    enriched_file_info["checksum_xxh"] = xxhash
    enriched_file_info["checksum_date"] = str(datetime.now())[:19]
            
    context.log.info(f"Checksum MD5: {md5} / Checksum XXHash: {xxhash}")
    return enriched_file_info
