import os
import utils
import proxy_utils as ut
from pathlib import Path
from dagster import op, Out

MP4_POLICY = os.environ["MP4_POLICY"]

@op(
    out={"proxy_result": Out(dict)},
    tags={"dagster-celery/queue": "encoding"},
)
def encode_proxy_mp4(context, file_info: dict, encoding_config) -> dict:
    source_path = file_info["file_path"]
    filename_stem = Path(source_path).stem
    filename = Path(source_path).name

    # Check file type first
    mime = file_info["mime_type"]
    if mime != "video":
        context.log.info("MIME type is not Video and cannot be transcoded...")
        return {
            "file_id": file_info.get("file_id")
        }

    # Get input date from Media dB here for path
    output_dir = Path(encoding_config.proxy_output_path)
    input_date = utils.get_media_input_date(filename)
    output_path = output_dir / f"{input_date}" / f"{filename_stem}.mp4"
    output_dir.mkdir(parents=True, exist_ok=True)
    if os.path.exists(output_path):
        confirm_finished = check_mod_time(output_path)
        if confirm_finished:
            os.remove(output_path)

    context.log.info(
        f"Encoding proxy MP4: {source_path} -> {output_path} "
        f"(threads: {encoding_config.thread_count})"
    )

    # Get additional metadata for decision making
    height = ut.get_height(source_path)
    width = ut.get_width(source_path)
    media_priref = file_info["cid_media_priref"]
   
    # Continue getting metadata here JMW
    dar: str,
    par: str,
    audio: Optional[str],
    default: Optional[str],
    vs: str,
    mixed_dict: Optional[dict[str, int]],
    fl_fr: bool,
    twelve_chnl: bool,

    height = int(height)
    width = int(width)
    aspect = round(width / height, 3)

    if vs:
        print(f"VS {vs}")
    map_video = ["-map", f"0:v:{vs}" if vs else "0:v:0"]

    map_audio = _build_audio_args(audio, default, mixed_dict, fl_fr, twelve_chnl)
    vf_args = _select_video_filter(height, width, aspect, dar, par)

    print(f"Audio command chosen: {map_audio}")
    print(f"Middle command chosen: {vf_args}")

    # No video filter matched and audio is present → can't build a valid command
    if not vf_args and audio is not None:
        return None

    base = (
        ["ffmpeg", "-i", fullpath]
        + map_video
        + ["-c:v", "libx264", "-crf", "28", "-pix_fmt", "yuv420p"]
    )
    output = ["-nostdin", "-y", output_path, "-f", "null", "-"]

    if audio is None:
        return base + ["-movflags", "faststart"] + vf_args + output
    ffmpeg_cmd = base+ vf_args + map_audio + ["-movflags", "faststart"] + output




    if result.returncode != 0:
        context.log.error(f"FFmpeg stderr: {result.stderr}")
        raise RuntimeError(f"FFmpeg encoding failed for {source_path}")

    proxy_size = output_path.stat().st_size
    context.log.info(f"Proxy created: {output_path} ({proxy_size} bytes)")

    return {
        "file_id": file_info.get("file_id"),
        "source_path": source_path,
        "proxy_path": str(output_path),
        "proxy_size": proxy_size,
    }
