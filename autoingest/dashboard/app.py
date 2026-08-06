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

tabs = st.tabs(["Overview", "Performance", "Throughput", "Storage Types", "Errors", "File Lookup"])


# ─── Tab 1: Overview ───

with tabs[0]:
    st.caption(f"Auto-refreshes every {REFRESH_SECONDS}s. Last update: "
               f"{pd.Timestamp.now().strftime('%H:%M:%S')}")

    today = queries.fetch_today_totals()
    storage_status = queries.fetch_storage_status_24h()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Files today", today[0] if today else 0)
    c2.metric("Completed", today[1] if today else 0)
    c3.metric("Errored", today[2] if today else 0)
    c4.metric("GB processed", f"{float(today[3]) / 1e9:.1f}" if today and today[3] else "0")
    c5.metric("Avg encode", f"{today[4] / 60:.1f} min" if today and today[4] else "—")
    c6.metric("Avg total", f"{today[5] / 60:.1f} min" if today and today[5] else "—")

    if storage_status:
        st.plotly_chart(
            charts._storage_stacked_bar(storage_status),
            width="stretch",
        )
    else:
        st.info("No files processed in the last 24 hours.")

    if st.button("Force refresh", key="refresh_overview"):
        st.rerun()


# ─── Tab 2: Performance ───

with tabs[1]:
    st.caption(f"Showing up to {MAX_ROWS} rows.")

    encode_data = queries.fetch_encode_performance()
    stage_data = queries.fetch_stage_timings()

    st.plotly_chart(
        charts.encode_histogram(encode_data),
        width="stretch",
    )
    st.plotly_chart(
        charts.stage_timing_bar(stage_data),
        width="stretch",
    )

    st.subheader("Stage Timing Summary")
    if stage_data:
        df_stage = pd.DataFrame(stage_data)
        df_stage["avg_sec"] = df_stage["avg_sec"].apply(lambda v: f"{float(v):.1f}" if v else "")
        df_stage["min_sec"] = df_stage["min_sec"].apply(lambda v: f"{float(v):.1f}" if v else "")
        df_stage["max_sec"] = df_stage["max_sec"].apply(lambda v: f"{float(v):.1f}" if v else "")
        st.dataframe(df_stage, width="stretch", hide_index=True)

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
            width="stretch",
            hide_index=True,
        )

    if st.button("Force refresh", key="refresh_perf"):
        st.rerun()


# ─── Tab 3: Throughput ───

with tabs[2]:
    throughput_hour = queries.fetch_throughput_by_hour()

    st.caption(
        "Shows all files updated in the last 7 days (any status — not just "
        "'All stages complete'). Includes files at every pipeline stage."
    )

    if throughput_hour:
        df_tp = pd.DataFrame(throughput_hour)
        total_files = int(df_tp["files"].sum())
        total_gb = float(df_tp["bytes_processed"].sum()) / 1e9

        c1, c2 = st.columns(2)
        c1.metric("Total files (7 days)", f"{total_files:,}")
        c2.metric("Total GB (7 days)", f"{total_gb:.1f}")

        st.plotly_chart(
            charts.throughput_line(throughput_hour),
            width="stretch",
        )

        # Per-storage summary table
        storage_summary = df_tp.groupby("storage", as_index=False).agg(
            files=("files", "sum"),
            bytes_processed=("bytes_processed", "sum"),
        )
        storage_summary["GB"] = (
            storage_summary["bytes_processed"] / 1e9
        ).round(2)
        storage_summary = storage_summary.sort_values(
            "GB", ascending=False
        ).drop(columns="bytes_processed")
        storage_summary.columns = ["Storage", "Files", "GB"]
        st.subheader("Per-Storage Breakdown (7 days)")
        st.dataframe(storage_summary, width="stretch", hide_index=True)

        # Per-storage dropdown with GB total
        storages = sorted(
            s for s in df_tp["storage"].dropna().unique() if s
        )
        if storages:
            selected = st.selectbox(
                "Break down by storage",
                options=["— All storages —"] + storages,
                key="throughput_storage",
            )
            if selected != "— All storages —":
                storage_gb = storage_summary.loc[
                    storage_summary["Storage"] == selected, "GB"
                ].values[0]
                storage_files = storage_summary.loc[
                    storage_summary["Storage"] == selected, "Files"
                ].values[0]
                st.caption(
                    f"{selected}: {storage_files:,} files, "
                    f"{storage_gb:.2f} GB"
                )
                st.plotly_chart(
                    charts.throughput_line(throughput_hour, storage=selected),
                    width="stretch",
                )
    else:
        st.info("No throughput data for the last 7 days.")

    if st.button("Force refresh", key="refresh_tp"):
        st.rerun()


# ─── Tab 4: Storage Types ───

with tabs[3]:
    filetype_data = queries.fetch_storage_filetype_counts()

    if filetype_data:
        df_ft = pd.DataFrame(filetype_data, columns=["storage", "file_fmt", "count"])
        storages = sorted(s for s in df_ft["storage"].dropna().unique() if s)

        selected = st.selectbox(
            "Filter by storage",
            options=["All storages"] + storages,
            key="storage_types_filter",
        )

        if selected != "All storages":
            filtered = queries.fetch_storage_filetype_counts(storage=selected)
            st.plotly_chart(
                charts.storage_filetype_blocks(filtered, storage=selected),
                width="stretch",
            )
        else:
            st.plotly_chart(
                charts.storage_filetype_blocks(filetype_data),
                width="stretch",
            )

        # Summary table
        st.subheader("File Type Counts")
        summary = df_ft.pivot_table(
            index="storage", columns="file_fmt",
            values="count", aggfunc="sum", fill_value=0,
        )
        st.dataframe(summary, width="stretch")
    else:
        st.info("No file type data available.")

    if st.button("Force refresh", key="refresh_st"):
        st.rerun()


# ─── Tab 5: Errors ───

with tabs[4]:
    error_counts = queries.fetch_error_distribution()
    error_files = queries.fetch_files_with_errors(100)

    st.plotly_chart(charts.error_bar(error_counts), width="stretch")

    st.subheader("Files with Errors")
    if error_files:
        df_err = pd.DataFrame(error_files)
        df_err["updated_at"] = df_err["updated_at"].apply(
            lambda t: str(t)[:19] if t else ""
        )
        st.dataframe(
            df_err[["file_name", "file_status", "error_message", "mime_type", "source", "updated_at"]],
            width="stretch",
            hide_index=True,
        )

    if st.button("Force refresh", key="refresh_err"):
        st.rerun()


# ─── Tab 6: File Lookup ───

with tabs[5]:
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

    st.subheader("Status Summary (all time)")
    status_df = pd.DataFrame(
        queries.fetch_status_counts(), columns=["Status", "Count"]
    )
    st.dataframe(status_df, width="stretch", hide_index=True)

    st.subheader("Source Summary")
    source_df = pd.DataFrame(
        queries.fetch_source_counts(), columns=["Source", "Count"]
    )
    st.dataframe(source_df, width="stretch", hide_index=True)
