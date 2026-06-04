import os
import time
import autoingest.resources.utils as utils
import autoingest.resources.proxy_utils as ut
from pathlib import Path
from dagster import op, Out

MP4_POLICY = os.environ.get("MP4_POLICY")

@op(
    out={"proxy_result": Out(dict)},
    tags={"dagster-celery/queue": "encoding"},
)
def encode_proxy_mp4(
    context,
    file_info: dict,
    encoding_config,
    workflow_db
) -> dict:
    source_path = file_info["file_path"]
    filename_stem = Path(source_path).stem
    filename = Path(source_path).name

    # Check file type first
    mime = file_info["mime_type"]
    source = file_info["source"]
    if mime != "video":
        context.log.info("MIME type is not Video and cannot be transcoded...")
        return {
            "file_id": file_info.get("file_id"),
            "source_path": source_path,
            "source": source,
            "proxy_video_path": "",
            "proxy_size": "",
        }
    # Check and block non-BFI sources
    if source.lower() in ["netflix", "amazon", "disney"]:
        context.log.info(f"Source is {source}... No transcode required.")
        return {
            "file_id": file_info.get("file_id"),
            "source_path": source_path,
            "source": source,
            "proxy_video_path": "",
            "proxy_size": "",
        }

    # Get input date from Media dB here for path
    output_dir = Path(encoding_config.proxy_output_path)
    input_date = utils.get_media_input_date(filename)
    if not input_date:
        context.log.info(f"Input date for {filename} Digital Media record not reachable.")
        raise RuntimeError(f"Input date could not be found in Digital Media record - {filename}")

    output_path = output_dir / f"{input_date}" / f"{filename_stem}.mp4"
    output_dir.mkdir(parents=True, exist_ok=True)
    if os.path.exists(output_path):
        confirm_finished = ut.check_mod_time(output_path)
        if confirm_finished:
            os.remove(output_path)

    context.log.info(
        f"Encoding proxy MP4: {source_path} -> {output_path} "
        f"(threads: {encoding_config.thread_count})"
    )

    # Get all required metadata for decision making
    height = ut.get_height(source_path)
    width = ut.get_width(source_path)
    aspect = round(width / height, 3)
    dar = ut.get_dar(source_path)
    par = ut.get_par(source_path)
    audio, stream_default, stream_count = ut.check_audio(source_path)
    duration, vs = ut.get_duration(source_path)
    mixed_dict = ut.check_for_mixed_audio(source_path)
    fl_fr = ut.check_for_fl_fr(source_path)
    twelve_channel = ut.check_for_twelve_channel_audio(source_path)

    context.log.info(f"""
        Metadata extracted: {height}x{width}, DAR {dar}, PAR {par}, \
        Aspect {aspect}, Duration {duration}, VS {vs}, Audio: {audio} - Stream default {stream_default} \
        - Stream count {stream_count} - FL/FR {fl_fr} - Twelve channel {twelve_channel} \
        Mixed audio dict: {mixed_dict}.
        """
    )

    # Start building FFmpeg command
    map_video = ["-map", f"0:v:{vs}" if vs else "0:v:0"]
    map_audio = ut.build_audio_args(audio, stream_default, mixed_dict, fl_fr, twelve_channel)
    vf_args = ut.select_video_filter(height, width, aspect, dar, par)

    context.log.info(f"Audio command chosen: {map_audio}")
    context.log.info(f"Middle command chosen: {vf_args}")

    # No video filter matched and audio is present → can't build a valid command
    if not vf_args and audio is not None:
        context.log.error("Cannot build valid command without video arguments")
        raise RuntimeError(f"FFmpeg command build failed for {source_path}")

    base = (
        ["ffmpeg", "-i", source_path]
        + map_video
        + ["-c:v", "libx264", "-crf", "28", "-pix_fmt", "yuv420p"]
    )
    output = ["-nostdin", "-y", output_path, "-f", "null", "-"]

    if audio is None:
        ffmpeg_cmd = base + vf_args + ["-movflags", "faststart"] + output
    else:
        ffmpeg_cmd = base + vf_args + map_audio + ["-movflags", "faststart"] + output

    ffmpeg_call_neat = " ".join(ffmpeg_cmd)
    context.log.info(f"FFmpeg command: {ffmpeg_call_neat}")
    tic = time.perf_counter()
    result = ut.call_ffmpeg_command(ffmpeg_cmd)
    toc = time.perf_counter()
    transcode_mins = (toc - tic) // 60
    context.log.info(f"FFmpeg encoding completed in: {transcode_mins} minutes")

    if result.returncode != 0 or not os.path.isfile(output_path):
        context.log.error(f"FFmpeg exit code {result.returncode} - stderr: {result.stderr}")
        raise RuntimeError(f"FFmpeg encoding failed for {source_path}")

    policy_check = utils.get_mediaconch(output_path, MP4_POLICY)
    if policy_check is True:
        proxy_size = output_path.stat().st_size
        context.log.info(f"Proxy created: {output_path} ({proxy_size} bytes)")
    else:
        os.remove(output_path)
        context.log.error(f"Deleted proxy - Mediaconch MP4 policy failed against new proxy file: {output_path}")
        raise RuntimeError(f"FFmpeg encoding failed for {source_path}")

    # Make JPEG source from blackdetect avoidance
    jpeg_location = output_dir / f"{input_date}" / f"{filename_stem}.jpg"
    context.log.info(f"JPEG proxy for clean up to go here: {jpeg_location}")

    # Calculate seconds mark to grab screen
    seconds = ut.adjust_seconds(duration, result)
    print(f"Seconds for JPEG cut: {seconds}")
    success = ut.get_jpeg(seconds, output_path, jpeg_location)
    if not success:
        context.log.error("Failed to make JPEG image - try again in next script")

    context.log.info("Updating Proxy Video data to dB")
    workflow_db.update_file_status(
        file_info["file_id"],
        {
            "proxy_video_path": str(output_path),
            "proxy_size": proxy_size,
            # All other metadata here
        }
    )

    return {
        "file_id": file_info.get("file_id"),
        "source_path": source_path,
        "source": source,
        "proxy_video_path": str(output_path),
        "proxy_size": proxy_size,
    }
