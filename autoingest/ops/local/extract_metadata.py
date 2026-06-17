import time
import json
from pathlib import Path
from typing import Any

import autoingest.resources.utils as utils
from datetime import datetime
from dagster import op, Out, Output, OpExecutionContext


@op(
    out=Out(dict),
    required_resource_keys={"workflow_db"},
)
def extract_metadata(context: OpExecutionContext, file_info: dict[str, Any]) -> Output:
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

    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app.file_catalogue
                SET mdata_full_text = %s,
                    mdata_text = %s,
                    mdata_ebucore = %s,
                    mdata_pbcore = %s,
                    mdata_full_xml = %s,
                    mdata_full_json = %s::jsonb,
                    updated_at = NOW()
                WHERE file_name = %s
            """, (
                file_info.get("mdata_full_text"),
                file_info.get("mdata_text"),
                file_info.get("mdata_ebucore"),
                file_info.get("mdata_pbcore"),
                file_info.get("mdata_full_xml"),
                file_info.get("mdata_full_json"),
                file_name,
            ))

    toc = time.perf_counter()
    duration_sec = round(toc - tic, 3)

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
