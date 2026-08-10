from importlib import metadata
from itertools import islice
import os
import time
import json
from pathlib import Path
from typing import Any

from dagster import op, OpExecutionContext, Output

from ...resources import utils
from ...resources import adlib

CID_API = utils.get_current_api()

FIELDS = [
    {"container.duration": ["Duration_String1", "Duration  "]},
    {"container.duration.seconds": ["Duration", ""]},
    {"container.file_size.total_bytes": ["FileSize", "File size  "]},
    {"container.file_size.total_gigabytes": ["FileSize_String4", "File size  "]},
    {"container.commercial_name": ["Format_Commercial", "Commercial name  "]},
    {"container.format": ["Format", "Format  "]},
    {"container.audio_codecs": ["Audio_Codec_List", "Audio codecs "]},
    {"container.audio_stream_count": ["AudioCount", "Count of audio streams  "]},
    {"container.video_stream_count": ["VideoCount", "Count of video streams  "]},
    {"container.format_profile": ["Format_Profile", "Format profile  "]},
    {"container.format_version": ["Format_Version", "Format version  "]},
    {"container.encoded_date": ["Encoded_Date", "Encoded date  "]},
    {"container.frame_count": ["FrameCount", "Frame count  "]},
    {"container.frame_rate": ["FrameRate", "Frame rate  "]},
    {"container.overall_bit_rate": ["OverallBitRate_String", "Overall bit rate  "]},
    {
        "container.overall_bit_rate_mode": [
            "OverallBitRate_Mode",
            "Overall bit rate mode  ",
        ]
    },
    {"container.writing_application": ["Encoded_Application", "Writing application  "]},
    {"container.writing_library": ["Encoded_Library", "Writing library  "]},
    {"container.file_extension": ["FileExtension", "File extension  "]},
    {"container.media_UUID": ["UniqueID", "Unique ID  "]},
    {"container.truncated": ["IsTruncated", "Is truncated  "]},
    {"video.duration": ["Duration_String1", ""]},
    {"video.duration.seconds": ["Duration", "Duration  "]},
    {"video.bit_depth": ["BitDepth", "Bit depth  "]},
    {"video.bit_rate_mode": ["BitRate_Mode", "Bit rate mode  "]},
    {"video.bit_rate": ["BitRate_String", "Bit rate  "]},
    {"video.chroma_subsampling": ["ChromaSubsampling", "Chroma subsampling"]},
    {"video.compression_mode": ["Compression_Mode", "Compression mode  "]},
    {"video.format_version": ["Format_Version", "Format version  "]},
    {"video.frame_count": ["FrameCount", "Frame count  "]},
    {"video.frame_rate": ["FrameRate", "Frame rate  "]},
    {"video.frame_rate_mode": ["FrameRate_Mode", "Frame rate mode  "]},
    {"video.height": ["Height", "Height  "]},
    {"video.scan_order": ["ScanOrder_String", "Scan order  "]},
    {"video.scan_type": ["ScanType", "Scan type  "]},
    {
        "video.scan_type.store_method": [
            "ScanType_StoreMethod_String",
            "Scan type, store method  ",
        ]
    },
    {"video.standard": ["Standard", "Standard  "]},
    {"video.stream_size_bytes": ["StreamSize", "Stream size  "]},
    {"video.stream_order": ["StreamOrder", "StreamOrder  "]},
    {"video.width": ["Width", "Width  "]},
    {"video.format_profile": ["Format_Profile", "Format profile  "]},
    {"video.width_aperture": ["Width_CleanAperture", "Width clean aperture  "]},
    {"video.delay": ["Delay", "Delay  "]},
    {"video.format_settings_GOP": ["Format_Settings_GOP", "Format settings, GOP  "]},
    {"video.codec_id": ["CodecID", "Codec ID  "]},
    {"video.colour_space": ["ColorSpace", "Color space  "]},
    {"video.colour_primaries": ["colour_primaries", "Color primaries  "]},
    {"video.commercial_name": ["Format_Commercial", "Commercial name  "]},
    {"video.display_aspect_ratio": ["DisplayAspectRatio", "Display aspect ratio  "]},
    {"video.format": ["Format", "Format  "]},
    {"video.matrix_coefficients": ["matrix_coefficients", "Matrix coefficients  "]},
    {"video.pixel_aspect_ratio": ["PixelAspectRatio", "Pixel aspect ratio  "]},
    {
        "video.transfer_characteristics": [
            "transfer_characteristics",
            "Transfer characteristics  ",
        ]
    },
    {"video.writing_library": ["Encoded_Library", "Writing library  "]},
    {"video.stream_size": ["StreamSize_String", "Stream size  "]},
    {"colour_range": ["colour_range", "Color range  "]},
    {"max_slice_count": ["MaxSlicesCount", "MaxSlicesCount  "]},
    {"audio.bit_depth": ["BitDepth", "Bit depth  "]},
    {"audio.bit_rate": ["BitRate_String", "Bit rate  "]},
    {"audio.bit_rate_mode": ["BitRate_Mode", "Bit rate mode  "]},
    {"audio.channels": ["Channels", "Channel(s)  "]},
    {"audio.codec_id": ["CodecID", "Codec ID  "]},
    {"audio.duration": ["Duration_String1", "Duration  "]},
    {"audio.channel_layout": ["ChannelLayout", "Channel layout  "]},
    {"audio.channel_position": ["ChannelPositions", "Channel positions  "]},
    {"audio.compression_mode": ["Compression_Mode", "Compression mode  "]},
    {
        "audio.format_settings_endianness": [
            "Format_Settings_Endianness",
            "Format settings, Endianness  ",
        ]
    },
    {"audio.format_settings_sign": ["Format_Settings_Sign", "Format settings, Sign  "]},
    {"audio.frame_count": ["FrameCount", "Frame count  "]},
    {"audio.language": ["Language_String", "Language  "]},
    {"audio.stream_size_bytes": ["StreamSize", "Stream size  "]},
    {"audio.stream_order": ["StreamOrder", "StreamOrder  "]},
    {"audio.stream_size": ["StreamSize_String", "Stream size  "]},
    {"audio.commercial_name": ["Format_Commercial", "Commercial name  "]},
    {"audio.format": ["Format", "Format  "]},
    {"audio.sampling_rate": ["SamplingRate_String", "Sampling rate  "]},
    {"other.duration": ["Duration_String1", "Duration  "]},
    {"other.frame_rate": ["FrameRate", "Frame rate  "]},
    {"other.language": ["Language_String", "Language  "]},
    {"other.type": ["Type", "Type  "]},
    {
        "other.timecode_first_frame": [
            "TimeCode_FirstFrame",
            "Time code of first frame  ",
        ]
    },
    {"other.stream_order": ["StreamOrder", "StreamOrder  "]},
    {"other.format": ["Format", "Format  "]},
    {"text.duration": ["Duration_String1", "Duration  "]},
    {"text.stream_order": ["StreamOrder", "StreamOrder  "]},
    {"text.format": ["Format", "Format  "]},
    {"text.codec_id": ["CodecID", "Codec ID  "]},
]


@op(required_resource_keys={"workflow_db"}, config_schema={"file_path": str})
def update_cid_metadata(context: OpExecutionContext) -> Output:
    tic = time.perf_counter()

    file_path_str = context.op_config["file_path"]
    file_name = Path(file_path_str).name

    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, file_name, file_status, mime_type, "
                "cid_media_priref, mdata_full_json, mdata_exif, "
                "mdata_text, mdata_full_text, mdata_full_xml, "
                "mdata_ebucore, mdata_pbcore "
                "FROM app.file_catalogue WHERE file_name = %s "
                "ORDER BY created_at DESC LIMIT 1",
                (file_name,),
            )
            row = cur.fetchone()

    if not row:
        context.log.error(f"File {file_name} not found in catalogue")
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"Not found: {file_name}",
        })

    file_id = row[0]
    file_status = row[2]
    mime_type = row[3]
    media_priref = row[4] or ""
    db_metadata = {
        "MediaInfo json 0": row[5],
        "Exiftool text": row[6] or "",
        "MediaInfo text 0": row[7] or "",
        "MediaInfo text 0 full": row[8] or "",
        "MediaInfo xml 0": row[9] or "",
        "MediaInfo ebucore 0": row[10] or "",
        "MediaInfo pbcore 0": row[11] or ""
    }

    if file_status == "updating_cid":
        context.log.info(f"File {file_name} is already being updated. Skipping.")
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"Already updating CID: {file_name}",
        })
    if file_status != "complete":
        context.log.info(
            f"File {file_name} has status '{file_status}' — expected 'complete'. Skipping."
        )
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"Skipped: status={file_status}",
        })

    if not media_priref:
        context.log.warning(
            f"No CID media priref for {file_name} — cannot update metadata."
        )
        _set_error_and_log(
            context, db, file_id, file_name, tic,
            error=f"CID media metadata update failed for file {file_name}",
            stage="no_priref",
            extra={"media_priref": ""},
        )
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"No media priref: {file_name}",
        })

    if not utils.cid_check(CID_API):
        context.log.warning("CID API is not responsive for metadata update")
        _set_error_and_log(
            context, db, file_id, file_name, tic,
            error=f"CID media metadata update failed for file {file_name}",
            stage="cid_down",
            extra={"media_priref": media_priref},
        )
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"CID API down: {file_name}",
        })

    _set_updating_status(db, file_id)

    # Add to header tags
    payload_data = ""
    for key, value in db_metadata.items():
        if len(value) > 0:
            try:
                text = f'<Header_tags><header_tags.parser>{key}</header_tags.parser><header_tags><![CDATA[{str(value)}]]></header_tags></Header_tags>'
                payload_data += text
            except Exception as err:
                print(err)
    if len(payload_data) > 10:
        payload = f"<adlibXML><recordList><record priref='{media_priref}'>{payload_data}</record></recordList></adlibXML>"
        context.log.info(f"Writing header tag data to CID Media record: {media_priref}\n{payload}")
        cid_tic = time.perf_counter()
        success, response = write_payload(payload, "media")
        cid_toc = time.perf_counter()
        cid_update_time = round(cid_toc - cid_tic, 3)
    else:
        context.log.warning(f"Failed to push metadata to header tags in rec {media_priref}:\n{payload_data}")
        cid_update_time = 0

    mime_type = (mime_type or "").lower()
    payload_xml = ""
    payload_source = ""

    if mime_type in ("video", "audio"):
        if not next(islice(db_metadata.values(), 0, None)):
            context.log.warning(
                f"No MediaInfo JSON data for {file_name} — advancing status."
            )
            _advance_status(context, db, file_id, file_name, tic, "All stages complete")
            return Output(None, metadata={
                "duration_sec": round(time.perf_counter() - tic, 3),
                "preview": f"No JSON metadata: {file_name}",
            })

        payload_xml = build_metadata_xml_from_db(next(islice(db_metadata.values(), 0, None)), media_priref)
        payload_source = "mediainfo_json"

    elif mime_type == "image":
        if not next(islice(db_metadata.values(), 1, None)):
            context.log.warning(
                f"No ExifTool data for {file_name} — advancing status."
            )
            _advance_status(context, db, file_id, file_name, tic, "All stages complete")
            return Output(None, metadata={
                "duration_sec": round(time.perf_counter() - tic, 3),
                "preview": f"No EXIF metadata: {file_name}",
            })

        payload_xml = build_exif_metadata_xml_from_db(next(islice(db_metadata.values(), 1, None)), media_priref)
        payload_source = "exiftool"

    else:
        context.log.info(
            f"MIME type '{mime_type}' for {file_name} — no technical metadata to update. "
            "Advancing to All stages complete."
        )
        _advance_status(context, db, file_id, file_name, tic, "All stages complete")
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"No applicable metadata for mime_type={mime_type}: {file_name}",
        })

    if not payload_xml:
        context.log.warning(
            f"Could not build metadata XML for {file_name} ({payload_source})."
            "Payload was empty after field extraction."
        )
        _set_error_and_log(
            context, db, file_id, file_name, tic,
            error=f"CID media metadata update failed for file {file_name}",
            stage="empty_payload",
            extra={"media_priref": media_priref, "source": payload_source},
        )
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "preview": f"Empty payload: {file_name}",
        })

    cid_tic = time.perf_counter()
    success, response = write_payload(payload_xml, "media")
    cid_toc = time.perf_counter()
    cid_update2_time = round(cid_toc - cid_tic, 3)
    cid_update_time += cid_update2_time

    if not success:
        context.log.warning(
            f"Failed to push metadata field data rec {media_priref}:\n{response}"
        )
        _set_error_and_log(
            context, db, file_id, file_name, tic,
            error=f"CID media metadata update failed for file {file_name}",
            stage="post_failure",
            extra={
                "media_priref": media_priref,
                "source": payload_source,
                "cid_update_time_sec": cid_update_time,
                "xml_payload": payload_xml,
                "cid_response": str(response),
            },
        )
        return Output(None, metadata={
            "duration_sec": round(time.perf_counter() - tic, 3),
            "cid_update_time_sec": cid_update_time,
            "preview": f"CID POST failed: {file_name}",
        })

    context.log.info(
        f"Metadata from {payload_source} written to CID media record {media_priref}"
    )
    _advance_status(
        context, db, file_id, file_name, tic, "All stages complete",
        extra={
            "cid_update_time_sec": cid_update_time,
            "media_priref": media_priref,
            "source": payload_source,
        },
    )

    duration_sec = round(time.perf_counter() - tic, 3)
    return Output(None, metadata={
        "duration_sec": duration_sec,
        "file_name": file_name,
        "cid_update_time_sec": cid_update_time,
        "media_priref": media_priref,
        "source": payload_source,
        "preview": f"{file_name} metadata updated in {duration_sec}s",
    })


def build_metadata_xml_from_db(mdata: Any, priref: str) -> str:
    if isinstance(mdata, str):
        try:
            mdata = json.loads(mdata)
        except (json.JSONDecodeError, TypeError):
            return ""

    if not isinstance(mdata, dict):
        return ""

    tracks = {}
    try:
        for track in mdata["media"]["track"]:
            track_type = track.get("@type")
            if track_type:
                tracks[track_type] = track
    except (KeyError, TypeError, IndexError):
        return ""

    xml_dct = {
        "General": "container",
        "Video": "video",
        "Audio": "audio",
        "Other": "other",
        "Text": "text",
    }

    payload = ""
    for key, value in xml_dct.items():
        track = tracks.get(key)
        if not track:
            continue
        if key == "Video":
            xml = get_video_xml(track)
        else:
            xml = get_xml(value, track)
        if xml:
            wrapped = wrap_as_xml(value.title(), xml)
            payload += wrapped

    if not payload:
        return ""

    return (
        f"<adlibXML><recordList><record priref='{priref}'>"
        f"{payload}</record></recordList></adlibXML>"
    )


def build_exif_metadata_xml_from_db(exif_text: str, priref: str) -> str:
    if not exif_text:
        return ""

    exif_lines = exif_text.strip().split("\n")
    if not exif_lines:
        return ""

    img_xml = get_image_xml(exif_lines)
    if not img_xml:
        return ""

    return adlib.create_record_data(CID_API, "media", priref, img_xml)


def get_xml(arg: str, track: dict) -> list[dict]:
    dct = []
    for field in FIELDS:
        for k, v in field.items():
            if k.startswith(f"{arg}."):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    if selected is None:
                        continue
                    dct.append({f"{k}": selected.strip()})
    return dct


def get_video_xml(track: dict) -> list[dict]:
    video_dict = []
    for field in FIELDS:
        for k, v in field.items():
            if k.startswith("video."):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    if selected is None:
                        continue
                    video_dict.append({f"{k}": selected.strip()})
            if k.startswith("colour_range"):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    if selected is None:
                        continue
                    video_dict.append({f"{k}": selected.strip()})
            if k.startswith("max_slice_count"):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    if selected is None:
                        continue
                    video_dict.append({f"{k}": selected.strip()})
                elif track.get("extra"):
                    try:
                        selected = manipulate_data(k, track["extra"].get(v[0]))
                        if selected is None:
                            continue
                        video_dict.append({f"{k}": selected.strip()})
                    except (KeyError, AttributeError, TypeError):
                        pass
    return video_dict


def get_image_xml(track: list[str]) -> list[dict[str, str]]:
    if not isinstance(track, list):
        return []

    data = [
        "File Size, file_size",
        "Bits Per Sample, bits_per_sample",
        "Color Components, colour_components",
        "Color Space, colour_space",
        "Compression, compression",
        "Encoding Process, encoding_process",
        "Exif Byte Order, exif_byte_order",
        "File Type, file_type",
        "Image Height, height",
        "Image Width, width",
        "Orientation, orientation",
        "Resolution Unit, resolution_unit",
        "Software, software",
        "X Resolution, x_resolution",
        "Y Cb Cr Sub Sampling, y_cb_cr_sub_sampling",
        "Y Resolution, y_resolution",
    ]

    image_dict = []
    for mdata in track:
        try:
            field, value = mdata.split(":", 1)
        except ValueError:
            continue
        for d in data:
            exif_field, cid_field = d.split(", ")
            if exif_field == field.strip() or exif_field in field.strip():
                image_dict.append({f"image.{cid_field}": value.strip()})

    return image_dict


def manipulate_data(key: str, selection: str) -> str | None:
    if selection is None:
        selection = ""

    if ".format_settings_endianness" in key and "big" in selection.lower():
        return "BIG"
    if ".format_settings_endianness" in key and "little" in selection.lower():
        return "LITTLE"
    if ".format" in key and " / " in selection:
        return selection.split(" / ")[0].strip()
    if ".audio_codecs" in key and " / " in selection:
        all_codecs = selection.split(" / ")
        unique_codecs = list(set(all_codecs))
        return ", ".join(unique_codecs)
    if ".codec_id" in key and " / " in selection:
        return selection.split(" / ")[0].strip()
    if ".sampling_rate" in key and selection.isnumeric():
        return None
    if ".stream_size_bytes" in key and selection.isnumeric():
        return selection
    if ".stream_size" in key and selection.isnumeric():
        return None
    if ".bit_rate" in key and selection.isnumeric():
        return None
    if selection == "Variable":
        return "VBR"
    if selection == "Constant":
        return "CBR"
    if selection == "Lossless":
        return "LOSSLESS"
    if selection == "Lossy":
        return "LOSSY"
    if selection == "Interlaced":
        return "INTER"
    if selection == "Progressive":
        return "PROG"
    if selection.lower() == "bottom_field_first":
        return "BFF"
    if selection.lower() == "top field first":
        return "TFF"
    if ".total_gigabytes" in key and "GiB" in selection:
        return selection.split(" GiB")[0]
    elif ".total_gigabytes" in key and "MiB" in selection:
        return None
    elif ".total_gigabytes" in key and "KiB" in selection:
        return None
    elif ".total_gigabytes" in key and "TiB" in selection:
        return None
    elif ".total_gigabytes" in key and selection.isnumeric():
        return None
    if "FPS" in selection:
        return selection.split(" FPS")[0]
    if ".milliseconds" in key and selection.isnumeric():
        return selection
    elif ".milliseconds" in key and ":" in selection:
        return None
    elif ".milliseconds" in key and "min" in selection:
        return None
    if ".bit_depth" in key and " bits" in selection:
        return selection.split(" bits")[0]
    if "language" in key and selection == "en":
        return "English"
    if "language" in key and "nar" in selection:
        return None
    if ".height" in key and "pixel" in selection:
        return None
    if ".width" in key and "pixels" in selection:
        return None
    if "audio.channels" in key and "channel" in selection:
        return None
    return selection


def wrap_as_xml(grouping: str, field_pairs: list[dict]) -> str:
    mid = ""
    for grouped in field_pairs:
        for key, val in grouped.items():
            xml_field = f"<{key}>{val}</{key}>"
            mid += xml_field
    return f"<{grouping}>{mid}</{grouping}>"


def write_payload(payload: str, database: str) -> tuple[bool, Any]:
    try:
        record = adlib.post(CID_API, payload, database, "updaterecord")
    except Exception as err:
        return False, str(err)

    if record is None:
        return False, record
    if isinstance(record, dict) and "@attributes" in record:
        return True, record
    if isinstance(record, str) and "'error': {'message':" in record:
        return False, record
    return False, record


def _set_error_and_log(
    context: OpExecutionContext,
    db: Any,
    file_id: int,
    file_name: str,
    tic: float,
    error: str,
    stage: str,
    extra: dict | None = None,
) -> None:
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                db._retry_query(conn, cur,
                    "UPDATE app.file_catalogue SET file_status = 'complete', "
                    "error_message = %s, updated_at = NOW() "
                    "WHERE id = %s",
                    (error, file_id),
                    context.log,
                )
    except Exception as exc:
        context.log.error(f"Failed to write error status for {file_name}: {exc}")

    duration_sec = round(time.perf_counter() - tic, 3)

    metadata = {
        "duration_sec": duration_sec,
        "file_name": file_name,
        "stage": stage,
        "error": error,
        "preview": f"{file_name} metadata {stage} in {duration_sec}s",
    }
    if extra:
        metadata.update({k: str(v) for k, v in extra.items()})

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="update_cid_metadata",
            event_type="op_completed",
            status="failure",
            metadata=metadata,
            message=error,
        )
    except Exception as exc:
        context.log.warning(f"Failed to record pipeline event for {file_name}: {exc}")


def _set_updating_status(db, file_id: int) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app.file_catalogue SET file_status = 'updating_cid', "
                "error_message = NULL, updated_at = NOW() WHERE id = %s",
                (file_id,),
            )


def _advance_status(
    context: OpExecutionContext,
    db: Any,
    file_id: int,
    file_name: str,
    tic: float,
    new_status: str,
    extra: dict | None = None,
) -> None:
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                db._retry_query(conn, cur,
                    "UPDATE app.file_catalogue "
                    "SET file_status = %s, updated_to_cid = 'TRUE', "
                    "total_ingest_time_sec = EXTRACT(EPOCH FROM (NOW() - created_at)), "
                    "error_message = NULL, updated_at = NOW() "
                    "WHERE id = %s",
                    (new_status, file_id),
                    context.log,
                )
    except Exception as exc:
        context.log.error(f"Failed to advance status for {file_name}: {exc}")

    duration_sec = round(time.perf_counter() - tic, 3)

    metadata = {
        "duration_sec": duration_sec,
        "file_name": file_name,
        "new_status": new_status,
        "preview": f"{file_name} status -> {new_status} in {duration_sec}s",
    }
    if extra:
        metadata.update({k: str(v) for k, v in extra.items()})

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="update_cid_metadata",
            event_type="op_completed",
            status="success",
            metadata=metadata,
        )
    except Exception as exc:
        context.log.warning(f"Failed to record pipeline event for {file_name}: {exc}")
