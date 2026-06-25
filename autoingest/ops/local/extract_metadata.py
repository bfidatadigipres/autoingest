import time
import json
from pathlib import Path
from typing import Any

import autoingest.resources.utils as utils
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

    file_path = file_info.get("file_path")
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
            file_info[mtype] = mdata
        else:
            file_info[mtype] = mdata

    mime_type = file_info.get("mime_type", "")
    if mime_type == "image":
        exif_tic = time.perf_counter()
        exif_result = utils.exif_data(file_path)
        if exif_result:
            file_info["mdata_exif"] = "\n".join(exif_result)
        else:
            file_info["mdata_exif"] = ""
        exif_toc = time.perf_counter()
        mdata_times["mdata_exif"] = round(exif_toc - exif_tic, 3)
    else:
        file_info["mdata_exif"] = ""

    # Augment metadata to specific dB fields
    mdata_general = {
        "file_fmt": "Format",
        "video_codec": "Video_Codec_List",
        "audio_codec": "Audio_Codec_List",
        "writing_library": "Encoded_Application",
        "audio_format": "Audio_Format_List",
        "framerate": "FrameRate_String",
        "audio_ch_total": "Audio_Channels_Total",
        "audio_count": "AudioCount",
        "video_count": "VideoCount"
    }
    mdata_video = {   
        "height": "Height_String",
        "width": "Width_String",
        "colorspace": "ColorSpace",
        "bitdepth": "BitDepth",
        "video_duration": "Duration",
    }
    metadata = file_info.get("mdata_full_json")
    if not isinstance(metadata, dict):
        context.log.warning("JSON full metadata has not created DICT - no file specific metadata in record")
    else:
        context.log.info("Extracting metadata from full JSON file for database")
        general_track = video_track = {}
        try:
            for track in metadata["media"]["track"]:
                if track.get("@type") == "General":
                    general_track = track
                elif track.get("@type") == "Video":
                    video_track = track
        except (IndexError, KeyError, TypeError) as err:
            context.log.warning("Error accessing JSON metadata tracks")

        if general_track:
            try:
                for k, v in general_track.items():
                    for key, val in mdata_general.items():
                        file_info[key] = ""
                        if val == k:
                            file_info[key] = v
                            break
            except (IndexError, KeyError, TypeError) as err:
                context.log.warning("Error accessing JSON metadata tracks")

        if video_track:
            try:
                for k, v in video_track.items():
                    for key, val in mdata_video.items():
                        file_info[key] = ""
                        if val == k:
                            file_info[key] = v
                            break
            except (IndexError, KeyError, TypeError) as err:
                context.log.warning("Error accessing JSON metadata tracks")

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
                    mdata_exif = %s,
                    file_fmt = %s,
                    video_codec = %s,
                    audio_codec = %s,
                    writing_library = %s,
                    audio_format = %s,
                    framerate = %s,
                    audio_ch_total = %s,
                    audio_count = %s,
                    video_count = %s,
                    height = %s,
                    width = %s,
                    colorspace = %s,
                    bitdepth = %s,
                    video_duration = %s,
                    updated_at = NOW()
                WHERE file_name = %s
            """, (
                file_info.get("mdata_full_text"),
                file_info.get("mdata_text"),
                file_info.get("mdata_ebucore"),
                file_info.get("mdata_pbcore"),
                file_info.get("mdata_full_xml"),
                file_info.get("mdata_full_json"),
                file_info.get("mdata_exif"),
                file_info.get("file_fmt"),
                file_info.get("video_codec"),
                file_info.get("audio_codec"),
                file_info.get("writing_library"),
                file_info.get("audio_format"),
                file_info.get("framerate"),
                file_info.get("audio_ch_total"),
                file_info.get("audio_count"),
                file_info.get("video_count"),
                file_info.get("height"),
                file_info.get("width"),
                file_info.get("colorspace"),
                file_info.get("bitdepth"),
                file_info.get("video_duration"),
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
