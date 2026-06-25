import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


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
    times = df["encode_time_sec"].dropna()
    if times.empty:
        return go.Figure()
    fig = px.histogram(
        times,
        nbins=30,
        title="Encode Time Distribution",
        labels={"value": "Encode time (seconds)"},
    )
    return fig


def throughput_line(throughput: list[dict], period: str = "hour") -> go.Figure:
    df = pd.DataFrame(throughput)
    if df.empty:
        return go.Figure()
    time_col = "hour" if "hour" in df.columns else "day"
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df[time_col],
        y=df["files"],
        name="Files",
        marker_color="#4ecdc4",
    ))
    fig.add_trace(go.Scatter(
        x=df[time_col],
        y=df["bytes_processed"] / 1e9,
        name="GB processed",
        yaxis="y2",
        line=dict(color="#ff6b6b", width=2),
    ))
    fig.update_layout(
        title="Throughput Over Time",
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
