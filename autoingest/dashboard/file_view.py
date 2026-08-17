import streamlit as st
import pandas as pd
from autoingest.dashboard.queries import search_file, fetch_file_events
from autoingest.dashboard.charts import file_timing_bar


_COLUMN_LABELS = {
    "id": "ID",
    "file_name": "File Name",
    "file_status": "File Status",
    "file_path": "File Path",
    "error_message": "Error Message",
    "source": "Source",
    "do_ingest": "Do Ingest",
    "incomplete_scan": "Incomplete Scan",
    "screencraft_arch": "Screencraft Arch",
    "part": "Part",
    "whole": "Whole",
    "extension": "Extension",
    "ffprobe_exit": "FFprobe Exit Code",
    "mime_type": "MIME Type",
    "cid_item_priref": "CID Item Prief",
    "cid_file_type": "CID File Type",
    "cid_ob_num": "CID OB Number",
    "cid_media_priref": "CID Media Prief",
    "bp_bucket": "BP Bucket",
    "bucket_list": "Bucket List",
    "file_size": "File Size",
    "checksum_xxh": "Checksum XXH",
    "checksum_md5": "Checksum MD5",
    "checksum_date": "Checksum Date",
    "ingest_month": "Ingest Month",
    "mdata_text": "MediaInfo Text",
    "mdata_full_text": "MediaInfo Full Text",
    "mdata_full_xml": "MediaInfo Full XML",
    "mdata_ebucore": "MediaInfo EBUCore",
    "mdata_pbcore": "MediaInfo PBCore",
    "mdata_full_json": "MediaInfo Full JSON",
    "file_fmt": "File Format",
    "video_codec": "Video Codec",
    "audio_codec": "Audio Codec",
    "writing_library": "Writing Library",
    "audio_format": "Audio Format",
    "framerate": "Framerate",
    "audio_ch_total": "Audio Channels Total",
    "audio_count": "Audio Count",
    "video_count": "Video Count",
    "height": "Height",
    "width": "Width",
    "colorspace": "Color Space",
    "bitdepth": "Bit Depth",
    "video_duration": "Video Duration",
    "autoingest_path": "Autoingest Path",
    "bp_job_id": "BP Job ID",
    "put_type": "Put Type",
    "persisted_ok": "Persisted OK",
    "bp_etag": "BP ETag",
    "bp_length": "BP Length",
    "bp_version_id": "BP Version ID",
    "validated": "Validated",
    "reference_num": "Reference Number",
    "ffmpeg_command": "FFmpeg Command",
    "proxy_video_path": "Proxy Video Path",
    "proxy_size": "Proxy Size",
    "proxy_image_path": "Proxy Image Path",
    "proxy_thumb_path": "Proxy Thumb Path",
    "updated_to_cid": "Updated to CID",
    "source_deletion": "Source Deletion",
    "created_at": "Created At",
    "updated_at": "Updated At",
    "tape_verified": "Tape Verified",
    "proxy_created": "Proxy Created",
    "checksum_time_sec": "Checksum Time (sec)",
    "encode_time_sec": "Encode Time (sec)",
    "image_time_sec": "Image Time (sec)",
    "verify_time_sec": "Verify Time (sec)",
    "total_ingest_time_sec": "Total Ingest Time (sec)",
    "mdata_exif": "MediaInfo EXIF",
}

_TRUNCATE_FIELDS = {
    "mdata_text", "mdata_full_text", "mdata_full_xml", "mdata_ebucore",
    "mdata_pbcore", "mdata_full_json", "mdata_exif", "ffmpeg_command",
    "bucket_list", "file_path", "autoingest_path", "proxy_video_path",
    "proxy_image_path", "proxy_thumb_path", "error_message",
}


def _format_size(size_bytes):
    if not size_bytes:
        return ""
    size = int(size_bytes)
    if size >= 1073741824:
        return f"{size / 1073741824:.2f} GB"
    if size >= 1048576:
        return f"{size / 1048576:.2f} MB"
    return f"{size} B"


def _truncate(value, max_len=40):
    if value is None:
        return ""
    s = str(value)
    if len(s) > max_len:
        return s[:max_len] + "..."
    return s


def render():
    st.header("File Lookup")
    search = st.text_input(
        "Search by file name",
        placeholder="N_10897500_01of01...",
        key="file_search",
    )

    if not search:
        st.info("Enter a file name to view its pipeline history.")
        return

    results = search_file(search)
    if not results:
        st.warning(f"No files found matching '{search}'.")
        return

    if len(results) > 1:
        selected = st.selectbox(
            "Matching files",
            options=results,
            format_func=lambda r: f"{r['file_name']} — {r['file_status']} ({r.get('source', '')})",
            key="file_select",
        )
    else:
        selected = results[0]

    f = selected

    st.subheader("File Details")
    cols = st.columns(4)
    cols[0].metric("File", f.get("file_name", "—"))
    cols[1].metric("Status", f.get("file_status", "—"))
    cols[2].metric("Source", f.get("source", "—"))
    cols[3].metric("MIME type", f.get("mime_type", "—"))

    cols = st.columns(4)
    cols[0].metric("Size", _format_size(f.get("file_size")))
    cols[1].metric("MD5", (f.get("checksum_md5") or "")[:12])
    cols[2].metric("Codec", f.get("file_fmt") or "—")
    cols[3].metric("Resolution", f"{f.get('width', '')}×{f.get('height', '')}" if f.get("width") else "—")

    cols = st.columns(4)
    cols[0].metric("Created", str(f.get("created_at"))[:19] if f.get("created_at") else "—")
    cols[1].metric("Updated", str(f.get("updated_at"))[:19] if f.get("updated_at") else "—")
    cols[2].metric("Storage", f.get("file_path") or "—")
    cols[3].metric("Error", (f.get("error_message") or "—")[:80])

    st.divider()
    st.subheader("All Row Data")

    row_data = {}
    for col, value in f.items():
        label = _COLUMN_LABELS.get(col, col)
        if col in _TRUNCATE_FIELDS:
            row_data[label] = _truncate(value)
        elif isinstance(value, dict):
            row_data[label] = _truncate(str(value))
        else:
            row_data[label] = value if value is not None else ""

    df_all = pd.DataFrame([row_data])
    st.dataframe(df_all, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Pipeline Runs")

    events = fetch_file_events(f["file_name"])
    if not events:
        st.info("No pipeline events found for this file.")
        return

    run_ids = sorted({e["run_id"] for e in events})
    st.caption(f"Found {len(run_ids)} run(s) across {len(events)} event(s)")

    if run_ids:
        run_data = []
        for rid in run_ids:
            run_events = [e for e in events if e["run_id"] == rid]
            stages = ", ".join(sorted({e["op_name"] for e in run_events}))
            statuses = {e["status"] for e in run_events}
            s = "success" if statuses == {"success"} else "mixed" if "success" in statuses else "failure"
            run_data.append({
                "Run ID": rid[:16] + "…",
                "Full Run ID": rid,
                "Stages": stages,
                "Status": s,
            })
        df_runs = pd.DataFrame(run_data)
        st.dataframe(
            df_runs[["Run ID", "Stages", "Status"]],
            width="stretch",
            hide_index=True,
        )

    st.divider()
    st.subheader("Stage Timings")

    timing_data = []
    for e in events:
        if e.get("duration"):
            timing_data.append({
                "op": e["op_name"],
                "duration": float(e["duration"]),
                "status": e["status"],
                "run_id": e["run_id"][:16] + "…",
            })

    if timing_data:
        df_timing = pd.DataFrame(timing_data)
        st.dataframe(
            df_timing[["op", "duration", "status", "run_id"]],
            width="stretch",
            hide_index=True,
        )

        fig = file_timing_bar(events)
        st.plotly_chart(fig, width="stretch")

    st.divider()
    st.subheader("Raw Pipeline Events")
    with st.expander("View event data"):
        for e in events:
            st.caption(f"{str(e['created_at'])[:19]} | {e['op_name']} | {e['status']}")
            st.json({k: str(v)[:200] for k, v in e.items() if k != "metadata"})
            if e.get("metadata"):
                st.caption("metadata:")
                st.json(e["metadata"])
            st.divider()

    st.divider()
    st.subheader("MediaInfo Text")

    mdata_text = f.get("mdata_text")
    if mdata_text:
        st.text_area(
            "Full MediaInfo output",
            value=str(mdata_text),
            height=400,
            key="mdata_text_display",
        )
    else:
        st.info("No MediaInfo text data available for this file.")
