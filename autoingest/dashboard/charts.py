import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


_ERROR_STATUSES = {"Failed assessment", "Failed validation", "validation_failure"}

_COMPLETE_STATUSES = {
    "All stages complete", "complete", "encoding_complete", "verified",
    "metadata_updated",
}

_ERROR_REDS = [
    "#c0392b",  # dark red
    "#e74c3c",  # mid red
    "#f1948a",  # light red
]

_DISTINCT_COLORS = [
    "#1f77b4",  # blue
    "#ff7f0e",  # orange
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#bcbd22",  # olive
    "#17becf",  # cyan
    "#7f7f7f",  # gray
    "#009688",  # teal
    "#34495e",  # navy
    "#ff9896",  # salmon
    "#c5b0d5",  # plum
    "#aec7e8",  # sky
    "#c49c94",  # tan
    "#f7b6d2",  # rose
    "#dbdb8d",  # mint
    "#ff7043",  # coral
]


def _status_colour_map(df: pd.DataFrame) -> dict:
    mapping = {}
    error_i = 0
    other_i = 0
    for s in sorted(df["file_status"].unique()):
        if s == "verified":
            mapping[s] = "#2ecc71"  # light green
        elif s == "All stages complete":
            mapping[s] = "#1e8449"  # dark green
        elif s == "encoding_complete":
            mapping[s] = "#f1c40f"  # yellow
        elif s == "complete":
            mapping[s] = "#6c3483"  # dark purple
        elif s in _COMPLETE_STATUSES:
            mapping[s] = "#27ae60"  # green (metadata_updated)
        elif s in _ERROR_STATUSES:
            mapping[s] = _ERROR_REDS[error_i % len(_ERROR_REDS)]
            error_i += 1
        else:
            mapping[s] = _DISTINCT_COLORS[other_i % len(_DISTINCT_COLORS)]
            other_i += 1
    return mapping


def _storage_stacked_bar(storage_data: list[tuple]) -> go.Figure:
    """
    Horizontal stacked bar per storage with per-status breakdown.
    Error statuses get distinct red shades; all other statuses use a broad
    palette for maximum contrast.
    """
    df = pd.DataFrame(storage_data, columns=["storage", "file_status", "count"])
    if df.empty:
        return go.Figure()

    # Pivot: rows=storage, cols=file_status
    pivot = df.pivot_table(
        index="storage", columns="file_status",
        values="count", aggfunc="sum", fill_value=0,
    )

    fig = go.Figure()
    colours = _status_colour_map(df)

    for status in sorted(pivot.columns):
        fig.add_trace(go.Bar(
            name=status,
            y=pivot.index,
            x=pivot[status],
            orientation="h",
            marker_color=colours.get(status, "#95a5a6"),
        ))

    fig.update_layout(
        barmode="stack",
        title="Files Ingested — Last 24 Hours (by storage & status)",
        xaxis_title="Files",
        yaxis_title="Storage",
        height=max(300, len(pivot) * 35 + 120),
        legend=dict(orientation="h", y=1.12),
    )
    return fig


def status_bar(status_counts: list[tuple]) -> go.Figure:
    df = pd.DataFrame(status_counts, columns=["status", "count"])
    fig = px.bar(
        df,
        x="count",
        y="status",
        orientation="h",
        color="status",
        text="count",
        title="File Status Distribution",
    )
    fig.update_layout(showlegend=False, height=max(200, len(df) * 30 + 60))
    fig.update_traces(textposition="outside")
    return fig


def source_pie(source_counts: list[tuple]) -> go.Figure:
    df = pd.DataFrame(source_counts, columns=["source", "count"])
    fig = px.pie(
        df,
        names="source",
        values="count",
        title="Files by Source",
        hole=0.4,
    )
    return fig


def stage_timing_bar(stage_data: list[dict]) -> go.Figure:
    df = pd.DataFrame(stage_data)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["op_name"],
        x=df["avg_sec"],
        orientation="h",
        name="Average (s)",
        text=[f"{v:.1f}s" for v in df["avg_sec"]],
        textposition="outside",
    ))
    fig.update_layout(
        title="Average Stage Duration (seconds)",
        height=max(200, len(df) * 35 + 60),
        showlegend=False,
    )
    return fig


def encode_histogram(encode_data: list[dict]) -> go.Figure:
    df = pd.DataFrame(encode_data)
    if df.empty or "encode_time_sec" not in df.columns:
        return go.Figure()
    df = df.dropna(subset=["encode_time_sec"])
    if df.empty:
        return go.Figure()
    fig = px.histogram(
        df,
        x="encode_time_sec",
        nbins=30,
        color="storage",
        title="Encode Time Distribution (by storage)",
        labels={"encode_time_sec": "Encode time (seconds)", "storage": "Storage"},
    )
    fig.update_layout(legend=dict(orientation="h", y=1.12))
    return fig


def throughput_line(throughput: list[dict], storage: str = None) -> go.Figure:
    df = pd.DataFrame(throughput)
    if df.empty:
        return go.Figure()

    if storage and "storage" in df.columns:
        df = df[df["storage"] == storage]
    else:
        # Aggregate across all storages
        df = df.groupby("hour", as_index=False).agg(
            {"files": "sum", "bytes_processed": "sum"}
        )

    if df.empty:
        return go.Figure()

    time_col = "hour"
    title = (
        f"Throughput Over Last 7 Days — {storage}"
        if storage
        else "Throughput Over Last 7 Days (all storages)"
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df[time_col],
        y=df["files"],
        name="Files",
        marker_color="#4ecdc4",
    ))
    fig.add_trace(go.Scatter(
        x=df[time_col],
        y=df["bytes_processed"].apply(float) / 1e9,
        name="GB processed",
        yaxis="y2",
        line=dict(color="#ff6b6b", width=2),
    ))
    fig.update_layout(
        title=title,
        yaxis=dict(title="Files"),
        yaxis2=dict(title="GB", overlaying="y", side="right"),
        legend=dict(x=0.01, y=0.99),
    )
    return fig


def error_bar(error_counts: list[tuple]) -> go.Figure:
    df = pd.DataFrame(error_counts, columns=["error", "count"])
    df["short"] = df["error"].apply(lambda s: s[:60] + "..." if len(s) > 60 else s)
    fig = px.bar(
        df,
        x="count",
        y="short",
        orientation="h",
        title="Error Distribution (top 30)",
        hover_data=["error"],
    )
    fig.update_layout(height=max(300, len(df) * 28 + 60), showlegend=False)
    return fig


def latency_box(data: list[float]) -> go.Figure:
    if not data:
        return go.Figure()
    fig = px.box(
        data,
        title="Total Ingest Time Distribution (seconds)",
        labels={"value": "Seconds"},
    )
    return fig


def file_timing_bar(events: list[dict]) -> go.Figure:
    stages = []
    times = []
    for e in events:
        op = e.get("op_name", "unknown")
        dur = e.get("duration")
        if dur:
            dur = float(dur)
            stages.append(op)
            times.append(dur)

    if not stages:
        return go.Figure()

    df = pd.DataFrame({"stage": stages, "seconds": times})
    df = df.groupby("stage")["seconds"].sum().reset_index()
    df = df.sort_values("seconds")

    fig = px.bar(
        df,
        x="seconds",
        y="stage",
        orientation="h",
        text="seconds",
        title="Per-Stage Duration (seconds)",
    )
    fig.update_layout(showlegend=False, height=max(200, len(df) * 40 + 60))
    fig.update_traces(texttemplate="%{text:.1f}s", textposition="outside")
    return fig


# File-type colour palette — distinct colours per file format
_FILETYPE_COLOURS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#009688", "#34495e", "#ff9896", "#c5b0d5", "#aec7e8",
    "#c49c94", "#f7b6d2", "#dbdb8d", "#ff7043", "#9edae5",
    "#c7c7c7", "#8c6d31", "#6baed6", "#fd8d3c", "#74c476",
]


def storage_filetype_blocks(filetype_data: list[tuple], storage: str = None) -> go.Figure:
    """
    Horizontal stacked bar: one row per storage, segments = file_fmt.
    Each file type gets a distinct colour.
    """
    df = pd.DataFrame(filetype_data, columns=["storage", "file_fmt", "count"])
    if df.empty:
        return go.Figure()

    if storage:
        df = df[df["storage"] == storage]
        if df.empty:
            return go.Figure()
        title = f"File Types — {storage}"
    else:
        title = "File Types by Storage"

    pivot = df.pivot_table(
        index="storage", columns="file_fmt",
        values="count", aggfunc="sum", fill_value=0,
    )

    fmts = sorted(pivot.columns)
    colour_map = {fmt: _FILETYPE_COLOURS[i % len(_FILETYPE_COLOURS)] for i, fmt in enumerate(fmts)}

    fig = go.Figure()
    for fmt in fmts:
        fig.add_trace(go.Bar(
            name=fmt,
            y=pivot.index,
            x=pivot[fmt],
            orientation="h",
            marker_color=colour_map.get(fmt, "#95a5a6"),
            hovertemplate=f"<b>{fmt}</b><br>Count: %{{x}}<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        title=title,
        xaxis_title="Files",
        yaxis_title="Storage",
        height=max(250, len(pivot) * 35 + 120),
        legend=dict(orientation="h", y=1.12, itemsizing="constant"),
    )
    return fig
