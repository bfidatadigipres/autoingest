import json
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

MAX_QUEUED_PER_STAGE = 80   # skip tick when more files than this are waiting at this stage
MAX_NEW_PER_TICK = 50       # per-tick launch cap in drain mode


STATUS_FIELD_QUERY = {
    "assessed": {
        "job": ingest_celery_job,
        "op": "generate_checksum",
        "sensor_name": "ingest_chain_sensor",
        "active_limit": 80,
        "active_gate_statuses": ("generating_checksum",),
    },
    "checksummed": {
        "job": catalogue_local_job,
        "op": "create_catalogue_record",
        "sensor_name": "catalogue_chain_sensor",
        "active_limit": 80,
        "active_gate_statuses": ("cataloguing",),
    },
    "verified": {
        "job": encoding_celery_job,
        "op": "encode_proxy_mp4",
        "sensor_name": "encoding_chain_sensor",
        "statuses": ("verified",),
        "max_queued": 80,
        "active_limit": 80,
        "active_gate_statuses": ("encoding", "generating_images"),
    },
    "encoding_complete": {
        "job": cleanup_local_job,
        "op": "check_and_delete_source",
        "sensor_name": "cleanup_status_sensor",
        "active_limit": 80,
        "active_gate_statuses": ("deleting_source",),
    },
    "complete": {
        "job": metadata_update_local_job,
        "op": "update_cid_metadata",
        "sensor_name": "metadata_update_chain_sensor",
        "active_limit": 80,
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
        # Cursor: sorted list of file_ids already submitted for this stage.
        submitted_ids: set[int] = set()
        if context.cursor:
            try:
                raw = json.loads(context.cursor)
                if isinstance(raw, list):
                    submitted_ids = {int(v) for v in raw}
                elif isinstance(raw, dict):
                    # Upgrade old timestamp cursor — keep keys, drop timestamps.
                    submitted_ids = {int(k) for k in raw}
            except (json.JSONDecodeError, TypeError, ValueError):
                submitted_ids = set()

        db = context.resources.workflow_db
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                if statuses:
                    cur.execute(
                        "SELECT id, file_path FROM app.file_catalogue "
                        "WHERE file_status IN %s ORDER BY created_at ASC",
                        (statuses,),
                    )
                else:
                    cur.execute(
                        "SELECT id, file_path FROM app.file_catalogue "
                        "WHERE file_status = %s ORDER BY created_at ASC",
                        (status,),
                    )
                rows = cur.fetchall()

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
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM app.file_catalogue "
                        "WHERE file_status IN %s",
                        (active_statuses,),
                    )
                    active_count = cur.fetchone()[0]
            if active_count >= active_limit:
                context.log.info(
                    f"{sensor_name}: skipping tick — {active_count} files "
                    f"already active {active_statuses}, limit={active_limit}"
                )
                return []

        # Prune cursor: drop file_ids that have moved past this status.
        current_ids = {row[0] for row in rows}
        submitted_ids &= current_ids

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
            if file_id in submitted_ids:
                continue

            if per_tick_cap is not None and len(new_requests) >= per_tick_cap:
                context.log.info(
                    f"{sensor_name}: per-tick cap reached "
                    f"({per_tick_cap}), deferring remaining"
                )
                break

            run_key = f"{op_name}-{file_id}"
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
            submitted_ids.add(file_id)

        if new_requests:
            context.log.info(f"{sensor_name}: launching {len(new_requests)} run(s)")

        context.update_cursor(json.dumps(sorted(submitted_ids)))
        return new_requests

    return _sensor_fn


ingest_chain_sensor = _make_status_sensor("assessed", STATUS_FIELD_QUERY["assessed"])
catalogue_chain_sensor = _make_status_sensor("checksummed", STATUS_FIELD_QUERY["checksummed"])
encoding_chain_sensor = _make_status_sensor("verified", STATUS_FIELD_QUERY["verified"])
cleanup_status_sensor = _make_status_sensor("encoding_complete", STATUS_FIELD_QUERY["encoding_complete"])
metadata_update_chain_sensor = _make_status_sensor("complete", STATUS_FIELD_QUERY["complete"])
