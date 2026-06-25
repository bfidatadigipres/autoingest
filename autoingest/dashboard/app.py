import streamlit as st
import pandas as pd

from autoingest.dashboard.config import REFRESH_SECONDS, MAX_ROWS
from autoingest.dashboard import queries
from autoingest.dashboard import charts
from autoingest.dashboard import file_view


st.set_page_config(
    page_title="BFI Autoingest — Pipeline Monitor",
    page_icon="📊",
    layout="wide",
)

st.title("BFI National Archive — Autoingest Pipeline Monitor")

tabs = st.tabs(["Overview", "Performance", "Throughput", "Errors", "File Lookup"])


# ─── Tab 1: Overview ───

with tabs[0]:
    st.caption(f"Auto-refreshes every {REFRESH_SECONDS}s. Last update: "
               f"{pd.Timestamp.now().strftime('%H:%M:%S')}")

    today = queries.fetch_today_totals()
    status_counts = queries.fetch_status_counts()
    source_counts = queries.fetch_source_counts()
    recent_events = queries.fetch_recent_events(50)
    mime_counts = queries.fetch_mime_counts()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Files today", today[0] if today else 0)
    c2.metric("Completed", today[1] if today else 0)
    c3.metric("Errored", today[2] if today else 0)
    c4.metric("GB processed", f"{float(today[3]) / 1e9:.1f}" if today and today[3] else "0")
    c5.metric("Avg encode", f"{today[4] / 60:.1f} min" if today and today[4] else "—")
    c6.metric("Avg total", f"{today[5] / 60:.1f} min" if today and today[5] else "—")

    col_left, col_right = st.columns(2)
    with col_left:
        st.plotly_chart(charts.status_bar(status_counts), use_container_width=True)
    with col_right:
        st.plotly_chart(charts.source_pie(source_counts), use_container_width=True)

    st.subheader("Recent Activity")
    if recent_events:
        df_recent = pd.DataFrame(recent_events)
        df_recent["created_at"] = df_recent["created_at"].apply(
            lambda t: str(t)[:19] if t else ""
        )
        df_recent["duration"] = df_recent["duration"].apply(
            lambda d: f"{float(d):.1f}s" if d else ""
        )
        st.dataframe(
            df_recent[["op_name", "job_name", "status", "duration", "created_at"]],
            use_container_width=True,
            hide_index=True,
        )

    if st.button("Force refresh", key="refresh_overview"):
        st.rerun()


# ─── Tab 2: Performance ───

with tabs[1]:
    st.caption(f"Showing up to {MAX_ROWS} rows.")

    encode_data = queries.fetch_encode_performance()
    stage_data = queries.fetch_stage_timings()

    st.plotly_chart(charts.encode_histogram(encode_data), use_container_width=True)
    st.plotly_chart(charts.stage_timing_bar(stage_data), use_container_width=True)

    st.subheader("Stage Timing Summary")
    if stage_data:
        df_stage = pd.DataFrame(stage_data)
        df_stage["avg_sec"] = df_stage["avg_sec"].apply(lambda v: f"{float(v):.1f}" if v else "")
        df_stage["min_sec"] = df_stage["min_sec"].apply(lambda v: f"{float(v):.1f}" if v else "")
        df_stage["max_sec"] = df_stage["max_sec"].apply(lambda v: f"{float(v):.1f}" if v else "")
        st.dataframe(df_stage, use_container_width=True, hide_index=True)

    st.subheader("Slowest Encodes")
    if encode_data:
        df_enc = pd.DataFrame(encode_data)
        df_enc["size_gb"] = df_enc["file_size"].apply(
            lambda s: f"{float(s) / 1e9:.2f}" if s else ""
        )
        df_enc["encode_min"] = df_enc["encode_time_sec"].apply(
            lambda t: f"{float(t) / 60:.1f}" if t else ""
        )
        st.dataframe(
            df_enc[["file_name", "encode_min", "size_gb", "height", "width", "source"]].head(20),
            use_container_width=True,
            hide_index=True,
        )

    if st.button("Force refresh", key="refresh_perf"):
        st.rerun()


# ─── Tab 3: Throughput ───

with tabs[2]:
    throughput_hour = queries.fetch_throughput_by_hour()
    throughput_day = queries.fetch_throughput_by_day()
    latency_data = queries.fetch_latency_distribution()

    st.plotly_chart(
        charts.throughput_line(throughput_hour, "hour"),
        use_container_width=True,
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.plotly_chart(
            charts.throughput_line(throughput_day, "day"),
            use_container_width=True,
        )
    with col_b:
        st.plotly_chart(charts.latency_box(latency_data), use_container_width=True)

    if st.button("Force refresh", key="refresh_tp"):
        st.rerun()


# ─── Tab 4: Errors ───

with tabs[3]:
    error_counts = queries.fetch_error_distribution()
    error_files = queries.fetch_files_with_errors(100)

    st.plotly_chart(charts.error_bar(error_counts), use_container_width=True)

    st.subheader("Files with Errors")
    if error_files:
        df_err = pd.DataFrame(error_files)
        df_err["updated_at"] = df_err["updated_at"].apply(
            lambda t: str(t)[:19] if t else ""
        )
        st.dataframe(
            df_err[["file_name", "file_status", "error_message", "mime_type", "source", "updated_at"]],
            use_container_width=True,
            hide_index=True,
        )

    if st.button("Force refresh", key="refresh_err"):
        st.rerun()


# ─── Tab 5: File Lookup ───

with tabs[4]:
    file_view.render()


# ─── Sidebar ───

with st.sidebar:
    st.header("About")
    st.markdown(
        "Pipeline Monitor for the BFI National Archive autoingest system. "
        "Queries the workflow database in real time."
    )
    st.metric("Auto-refresh", f"{REFRESH_SECONDS}s")

    st.divider()

    today_status = queries.fetch_today_totals()
    all_status = queries.fetch_status_counts()

    st.subheader("Status Summary")
    status_df = pd.DataFrame(all_status, columns=["Status", "Count"])
    st.dataframe(status_df, use_container_width=True, hide_index=True)

    st.subheader("Source Summary")
    source_df = pd.DataFrame(queries.fetch_source_counts(), columns=["Source", "Count"])
    st.dataframe(source_df, use_container_width=True, hide_index=True)

    raw_events = queries.fetch_recent_events(10)
    if raw_events:
        st.subheader("Latest Events")
        for e in raw_events[:5]:
            st.caption(
                f"{str(e['created_at'])[:19]} | {e['op_name']} | {e.get('status', '')}"
            )
