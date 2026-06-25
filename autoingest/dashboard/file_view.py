import streamlit as st
import pandas as pd
from autoingest.dashboard.queries import search_file, fetch_file_events
from autoingest.dashboard.charts import file_timing_bar


def _format_size(size_bytes):
    if not size_bytes:
        return ""
    size = int(size_bytes)
    if size >= 1073741824:
        return f"{size / 1073741824:.2f} GB"
    if size >= 1048576:
        return f"{size / 1048576:.2f} MB"
    return f"{size} B"


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
            use_container_width=True,
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
            use_container_width=True,
            hide_index=True,
        )

        fig = file_timing_bar(events)
        st.plotly_chart(fig, use_container_width=True)

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
