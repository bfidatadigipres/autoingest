import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.ingest_jobs import ingest_celery_job, catalogue_local_job
from autoingest.jobs.validation_jobs import (
    encoding_celery_job,
    cleanup_local_job,
    metadata_update_local_job,
)

MAX_QUEUED_PER_STAGE = 30   # skip tick when more files than this are waiting at this stage
MAX_NEW_PER_TICK = 10       # per-tick launch cap in drain mode
RETRY_INTERVAL_SECONDS = 600  # retry stuck cursor entries after 10 min


STATUS_FIELD_QUERY = {
    "assessed": {
        "job": ingest_celery_job,
        "op": "generate_checksum",
        "sensor_name": "ingest_chain_sensor",
        "active_limit": 20,
        "active_gate_statuses": ("generating_checksum",),
    },
    "checksummed": {
        "job": catalogue_local_job,
        "op": "create_catalogue_record",
        "sensor_name": "catalogue_chain_sensor",
        "active_limit": 20,
        "active_gate_statuses": ("cataloguing",),
    },
    "verified": {
        "job": encoding_celery_job,
        "op": "encode_proxy_mp4",
        "sensor_name": "encoding_chain_sensor",
        "statuses": ("verified",),
        "max_queued": 30,
        "active_limit": 20,
        "active_gate_statuses": ("encoding", "generating_images"),
    },
    "encoding_complete": {
        "job": cleanup_local_job,
        "op": "check_and_delete_source",
        "sensor_name": "cleanup_status_sensor",
        "active_limit": 20,
        "active_gate_statuses": ("deleting_source",),
    },
    "complete": {
        "job": metadata_update_local_job,
        "op": "update_cid_metadata",
        "sensor_name": "metadata_update_chain_sensor",
        "active_limit": 20,
        "active_gate_statuses": ("updating_cid",),
    },
}


def _make_status_sensor(status: str, conf: dict[str, Any]) -> Callable[..., Any]:
    job = conf["job"]
    op_name = conf["op"]
    sensor_name = conf["sensor_name"]
    statuses = conf.get("statuses")

    @sensor(
        job=job,
        name=sensor_name,
        minimum_interval_seconds=30,
        default_status=DefaultSensorStatus.RUNNING,
        required_resource_keys={"workflow_db"},
    )
    def _sensor_fn(context: SensorEvaluationContext) -> list[RunRequest]:
        now = int(time.time())

        # Cursor: {file_id: launch_timestamp} — enables crash/retry recovery.
        cursor: dict[int, int] = {}
        if context.cursor:
            try:
                raw = json.loads(context.cursor)
                if isinstance(raw, dict):
                    cursor = {int(k): int(v) for k, v in raw.items()}
                elif isinstance(raw, list):
                    # Upgrade old list cursor — timestamps default to 0.
                    cursor = {int(v): 0 for v in raw}
            except (json.JSONDecodeError, TypeError, ValueError):
                cursor = {}

        db = context.resources.workflow_db
        rows = []
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    if statuses:
                        cur.execute(
                            "SELECT id, file_path FROM app.file_catalogue "
                            "WHERE file_status IN %s "
                            "ORDER BY created_at ASC LIMIT 500",
                            (statuses,),
                        )
                    else:
                        cur.execute(
                            "SELECT id, file_path FROM app.file_catalogue "
                            "WHERE file_status = %s "
                            "ORDER BY created_at ASC LIMIT 500",
                            (status,),
                        )
                    rows = cur.fetchall()
        except Exception as exc:
            context.log.warning(
                f"{sensor_name}: DB query failed, returning empty: {exc}"
            )
            return []

        max_queued = conf.get("max_queued", MAX_QUEUED_PER_STAGE)
        drain_mode = False
        if len(rows) > max_queued:
            context.log.info(
                f"{sensor_name}: drain mode — {len(rows)} files queued "
                f"for status '{status}', exceeds limit of {max_queued}"
            )
            drain_mode = True

        # Active-pipeline gate: count files already consuming compute
        # and skip launching if at or above the active limit.
        active_limit = conf.get("active_limit")
        active_count = 0
        if active_limit:
            active_statuses = conf.get("active_gate_statuses", ())
            try:
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) FROM app.file_catalogue "
                            "WHERE file_status IN %s",
                            (active_statuses,),
                        )
                        active_count = cur.fetchone()[0]
            except Exception as exc:
                context.log.warning(
                    f"{sensor_name}: active-gate check failed: {exc}"
                )
                return []
            if active_count >= active_limit:
                context.log.info(
                    f"{sensor_name}: drain mode (active) — {active_count} "
                    f"files already active {active_statuses}, "
                    f"limit={active_limit}"
                )
                drain_mode = True

        # Prune cursor: drop file_ids that have moved past this status,
        # or that have been stuck for longer than RETRY_INTERVAL_SECONDS.
        current_ids = {row[0] for row in rows}
        for file_id, ts in list(cursor.items()):
            if file_id not in current_ids:
                del cursor[file_id]
            elif (now - ts) > RETRY_INTERVAL_SECONDS:
                del cursor[file_id]
                context.log.info(
                    f"{sensor_name}: retrying file_id={file_id} "
                    f"after {now - ts}s in cursor"
                )

        if drain_mode:
            per_tick_cap = MAX_NEW_PER_TICK
        elif active_limit:
            per_tick_cap = max(active_limit - active_count, 0)
            if per_tick_cap == 0:
                context.log.info(
                    f"{sensor_name}: no launch room — {active_count} active, "
                    f"limit={active_limit}"
                )
                return []
        else:
            per_tick_cap = None

        new_requests = []
        for file_id, file_path in rows:
            if file_id in cursor:
                continue

            if per_tick_cap is not None and len(new_requests) >= per_tick_cap:
                context.log.info(
                    f"{sensor_name}: per-tick cap reached "
                    f"({per_tick_cap}), deferring remaining"
                )
                break

            run_key = f"{op_name}-{file_id}-{now}"
            context.log.info(
                f"{sensor_name}: launching {op_name} for file_id={file_id} "
                f"({Path(file_path).name})"
            )
            new_requests.append(
                RunRequest(
                    run_key=run_key,
                    run_config={
                        "ops": {
                            op_name: {"config": {"file_path": file_path}}
                        }
                    },
                )
            )
            cursor[file_id] = now

        if new_requests:
            context.log.info(f"{sensor_name}: launching {len(new_requests)} run(s)")

        context.update_cursor(json.dumps(cursor))
        return new_requests

    return _sensor_fn


ingest_chain_sensor = _make_status_sensor("assessed", STATUS_FIELD_QUERY["assessed"])
catalogue_chain_sensor = _make_status_sensor("checksummed", STATUS_FIELD_QUERY["checksummed"])
encoding_chain_sensor = _make_status_sensor("verified", STATUS_FIELD_QUERY["verified"])
cleanup_status_sensor = _make_status_sensor("encoding_complete", STATUS_FIELD_QUERY["encoding_complete"])
metadata_update_chain_sensor = _make_status_sensor("complete", STATUS_FIELD_QUERY["complete"])
