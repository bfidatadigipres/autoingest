"""
Mostly completed, for review later
- Creates blocks of metadata text and saves into file_info dB
- Creates checksums and saves into enriched_file_info dB
"""

import time
import json
from pathlib import Path
import autoingest.resources.utils as utils
from datetime import datetime
from dagster import op, Out, Output


@op(
    out=Out(dict),
    required_resource_keys={"workflow_db"},
)
def extract_metadata(context, file_info: dict) -> Output:
    tic = time.perf_counter()

    if file_info.get("do_ingest") != "TRUE":
        context.log.info("Skipping metadata extraction — file not cleared for ingest.")
        return Output(file_info, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

    file_path = file_info["file_path"]
    file_name = file_info.get("file_name", Path(file_path).name)
    context.log.info(f"** Extracting metadata from {file_path}")

    mdata_type = [
        "mdata_full_text",
        "mdata_text",
        "mdata_ebucore",
        "mdata_pbcore",
        "mdata_full_xml",
        "mdata_full_json"
    ]

    mdata_times = {}
    for mtype in mdata_type:
        mt_tic = time.perf_counter()
        mdata = utils.make_metadata(file_path, mtype)
        mt_toc = time.perf_counter()
        mdata_times[mtype] = round(mt_toc - mt_tic, 3)
        if "json" in mdata:
            metadata_str = json.dumps(mdata)
            file_info[mtype] = metadata_str
        else:
            file_info[mtype] = mdata

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)

    db = context.resources.workflow_db
    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="extract_metadata",
            event_type="op_completed",
            status="success",
            metadata={
                "duration_sec": duration_sec,
                "file_name": file_name,
                "mdata_times": mdata_times,
                "preview": f"{file_name} mediainfo extracted in {duration_sec}s",
            },
        )
    except Exception:
        pass

    return Output(file_info, metadata={
        "duration_sec": duration_sec,
        "file_name": file_name,
        "mdata_times": mdata_times,
        "preview": f"{file_name} mediainfo in {duration_sec}s",
    })


@op(
    tags={"dagster-celery/queue": "encoding"},
    out=Out(dict),
    required_resource_keys={"workflow_db"},
)
def generate_checksum(context, enriched_file_info: dict) -> Output:
    tic = time.perf_counter()

    if enriched_file_info.get("do_ingest") != "TRUE":
        context.log.info("Skipping checksum generation — file not cleared for ingest.")
        return Output(enriched_file_info, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

    file_path = enriched_file_info["file_path"]
    file_name = enriched_file_info.get("file_name", Path(file_path).name)
    file_size = enriched_file_info.get("file_size", 0)
    context.log.info(f"Generating MD5 checksum for {file_path}")

    md5_tic = time.perf_counter()
    md5 = utils.create_md5_65536(file_path)
    md5_toc = time.perf_counter()
    enriched_file_info["checksum_md5"] = md5

    xxh_tic = time.perf_counter()
    xxhash_val = utils.create_xxhash_65536(file_path)
    xxh_toc = time.perf_counter()
    enriched_file_info["checksum_xxh"] = xxhash_val
    enriched_file_info["checksum_date"] = str(datetime.now())[:19]

    md5_time = round(md5_toc - md5_tic, 3)
    xxh_time = round(xxh_toc - xxh_tic, 3)

    context.log.info(f"Checksum MD5: {md5} / Checksum XXHash: {xxhash_val}")

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)

    db = context.resources.workflow_db
    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="generate_checksum",
            event_type="op_completed",
            status="success" if md5 and xxhash_val else "failure",
            metadata={
                "duration_sec": duration_sec,
                "file_name": file_name,
                "file_size": file_size,
                "md5_time_sec": md5_time,
                "xxhash_time_sec": xxh_time,
                "md5_throughput_mbps": round(file_size / md5_time / 1048576, 2) if md5_time > 0 else 0,
                "xxh_throughput_mbps": round(file_size / xxh_time / 1048576, 2) if xxh_time > 0 else 0,
                "checksum_md5": md5[:8] if md5 else None,
                "preview": f"{file_name} checksums in {duration_sec}s (MD5: {md5_time}s, XXH: {xxh_time}s)",
            },
        )
    except Exception:
        pass

    return Output(enriched_file_info, metadata={
        "duration_sec": duration_sec,
        "file_name": file_name,
        "file_size": file_size,
        "md5_time_sec": md5_time,
        "xxhash_time_sec": xxh_time,
        "preview": f"{file_name} checksums in {duration_sec}s",
    })
