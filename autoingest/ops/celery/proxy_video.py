import os
import time
import autoingest.resources.utils as utils
import autoingest.resources.proxy_utils as ut
from pathlib import Path
from dagster import op, OpExecutionContext, Output

MP4_POLICY = os.environ.get("MP4_POLICY")


@op(
    required_resource_keys={"workflow_db", "encoding_config"},
    tags={"dagster-celery/queue": "encoding"},
    config_schema={"file_path": str},
)
def encode_proxy_mp4(
    context: OpExecutionContext,
) -> Output:
    tic = time.perf_counter()
    file_path = context.op_config["file_path"]
    filename_stem = Path(file_path).stem
    filename = Path(file_path).name

    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, mime_type, source, ingest_month, bp_job_id "
                "FROM app.file_catalogue "
                "WHERE file_name = %s ORDER BY created_at DESC LIMIT 1",
                (filename,),
            )
            row = cur.fetchone()

    if not row:
        context.log.error(f"No DB record found for {filename}")
        return Output({}, metadata={"duration_sec": round(time.perf_counter() - tic, 3)})

    file_id, mime_type, source, ingest_month, bp_job_id = row

    root = Path(file_path).parent.parent.parent.parent
    source_path = root / "autoingest" / "validate" / (bp_job_id or "") / filename
    if not source_path.is_file():
        raise RuntimeError(
            f"Source file not found at validation path: {source_path}. "
            f"Original ingest path ({file_path}) may be stale — file has moved through the pipeline."
        )

    file_path = str(source_path)

    context.log.info(f"Encoding proxy for {filename} ({mime_type}, source: {source})")

    if mime_type != "video":
        context.log.info("MIME type is not Video and cannot be transcoded...")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output({
            "file_id": file_id,
            "file_path": file_path,
            "source": source,
            "mime_type": mime_type,
            "proxy_video_path": "",
            "proxy_size": "",
        }, metadata={"duration_sec": duration_sec, "preview": f"Skipped (non-video): {filename}"})

    if source.lower() in ["netflix", "amazon", "disney"]:
        context.log.info(f"Source is {source}... No transcode required.")
        duration_sec = round(time.perf_counter() - tic, 3)
        return Output({
            "file_id": file_id,
            "file_path": file_path,
            "source": source,
            "mime_type": mime_type,
            "proxy_video_path": "",
            "proxy_size": "",
        }, metadata={"duration_sec": duration_sec, "preview": f"Skipped (non-BFI): {filename}"})

    cfg = context.resources.encoding_config
    output_dir = Path(cfg.proxy_output_path)
    if not ingest_month:
        context.log.error(
            f"Ingest month not set for {filename} — verification has not populated the field. "
            "Cannot determine proxy output path."
        )
        raise RuntimeError(
            f"No ingest_month for {filename}. Re-run verification to populate the field, "
            "or run: ALTER TABLE app.file_catalogue RENAME COLUMN ingest_folder TO ingest_month"
        )

    output_path = output_dir / f"{ingest_month}" / f"{filename_stem}.mp4"
    output_dir.mkdir(parents=True, exist_ok=True)
    if os.path.exists(output_path):
        confirm_finished = ut.check_mod_time(output_path)
        if confirm_finished:
            os.remove(output_path)

    context.log.info(
        f"Encoding proxy MP4: {file_path} -> {output_path} "
        f"(threads: {cfg.thread_count})"
    )

    height = ut.get_height(file_path)
    width = ut.get_width(file_path)
    if height == 0 or width == 0:
        context.log.warning(f"Height {height} or Width {width} failed to fetch. Exiting.")
        raise RuntimeError(f"Encoding failed - missing height and/or width - {width}:{height}")
    aspect = round(int(width) / int(height), 3)
    dar = ut.get_dar(file_path)
    par = ut.get_par(file_path)
    audio, stream_default, stream_count = ut.check_audio(file_path)
    duration, vs = ut.get_duration(file_path)
    mixed_dict = ut.check_for_mixed_audio(file_path)
    fl_fr = ut.check_for_fl_fr(file_path)
    twelve_channel = ut.check_for_twelve_channel_audio(file_path)

    context.log.info(f"""
        Metadata extracted: {height}x{width}, DAR {dar}, PAR {par}, \
        Aspect {aspect}, Duration {duration}, VS {vs}, Audio: {audio} - Stream default {stream_default} \
        - Stream count {stream_count} - FL/FR {fl_fr} - Twelve channel {twelve_channel} \
        Mixed audio dict: {mixed_dict}.
        """
    )

    map_video = ["-map", f"0:v:{vs}" if vs else "0:v:0"]
    map_audio = ut.build_audio_args(audio, stream_default, mixed_dict, fl_fr, twelve_channel)
    vf_args = ut.select_video_filter(height, width, aspect, dar, par)

    context.log.info(f"Audio command chosen: {map_audio}")
    context.log.info(f"Middle command chosen: {vf_args}")

    if not vf_args:
        context.log.error("Cannot build valid command — vf_args is empty")
        raise RuntimeError(f"FFmpeg command build failed for {file_path}: no video filter arguments could be determined")

    base = (
        [cfg.ffmpeg_path, "-i", file_path]
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
    ffmpeg_tic = time.perf_counter()
    result = ut.call_ffmpeg_command(ffmpeg_cmd)
    ffmpeg_toc = time.perf_counter()
    ffmpeg_time = round(ffmpeg_toc - ffmpeg_tic, 3)
    transcode_mins = ffmpeg_time // 60
    context.log.info(f"FFmpeg encoding completed in: {transcode_mins} minutes")

    if result.returncode != 0 or not os.path.isfile(output_path):
        stderr_snippet = (result.stderr or b"").decode("utf-8", errors="replace")[-500:] if result.stderr else "(none)"
        context.log.error(f"FFmpeg exit code {result.returncode} - stderr: {stderr_snippet}")
        raise RuntimeError(
            f"FFmpeg encoding failed for {file_path} (exit {result.returncode}). "
            f"Command: {ffmpeg_call_neat}. Stderr: {stderr_snippet}"
        )

    policy_check = utils.get_mediaconch(output_path, MP4_POLICY)
    if policy_check is True:
        proxy_size = output_path.stat().st_size
        source_size = os.path.getsize(file_path)
        compression_ratio = round(source_size / proxy_size, 2) if proxy_size > 0 else 0
        context.log.info(f"Proxy created: {output_path} ({proxy_size} bytes)")
    else:
        os.remove(output_path)
        context.log.error(f"Deleted proxy - Mediaconch MP4 policy failed against new proxy file: {output_path}")
        raise RuntimeError(f"FFmpeg encoding failed for {file_path}")

    jpeg_location = output_dir / f"{ingest_month}" / f"{filename_stem}.jpg"
    context.log.info(f"JPEG proxy for clean up to go here: {jpeg_location}")

    seconds = ut.adjust_seconds(duration, result.stderr if result.stderr else "")
    print(f"Seconds for JPEG cut: {seconds}")
    success = ut.get_jpeg(seconds, output_path, jpeg_location)
    if not success:
        context.log.error(
            f"Failed to extract JPEG from proxy: {jpeg_location}. "
            "Cannot proceed to image generation."
        )
        raise RuntimeError(f"JPEG extraction failed for {file_path}")

    context.log.info("Updating Proxy Video data to dB")
    db.update_file_status(
        file_id,
        proxy_video_path=str(output_path),
        proxy_size=proxy_size,
        ffmpeg_command=ffmpeg_call_neat,
        encode_time_sec=ffmpeg_time,
        error_message=None,
    )

    toc = time.perf_counter()
    total_duration_sec = round(toc - tic, 3)

    metadata = {
        "duration_sec": total_duration_sec,
        "file_name": filename,
        "ffmpeg_time_sec": ffmpeg_time,
        "source_size": source_size,
        "proxy_size": proxy_size,
        "compression_ratio": compression_ratio,
        "height": height,
        "width": width,
        "dar": dar,
        "par": par,
        "aspect": aspect,
        "duration": duration,
        "audio": audio,
        "stream_count": stream_count,
        "ffmpeg_command": ffmpeg_call_neat,
        "preview": f"{filename} encoded in {transcode_mins}min ({source_size}→{proxy_size}B, {compression_ratio}x)",
    }

    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="encode_proxy_mp4",
            event_type="op_completed",
            status="success",
            metadata=metadata,
        )
    except Exception:
        pass

    return Output({
        "file_id": file_id,
        "file_path": file_path,
        "source": source,
        "mime_type": mime_type,
        "proxy_video_path": str(output_path),
        "proxy_size": proxy_size,
    }, metadata={
        "duration_sec": total_duration_sec,
        "ffmpeg_time_sec": ffmpeg_time,
        "proxy_size": proxy_size,
        "source_size": source_size,
        "compression_ratio": compression_ratio,
        "height": height,
        "width": width,
        "ffmpeg_command": ffmpeg_call_neat,
        "preview": f"{filename} encoded {transcode_mins}min, {source_size}→{proxy_size}B",
    })
