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

RETRY_INTERVAL_SECONDS = 300  # re-trigger stuck files after 5 minutes


STATUS_FIELD_QUERY = {
    "assessed": {
        "job": ingest_celery_job,
        "op": "generate_checksum",
        "sensor_name": "ingest_chain_sensor",
    },
    "checksummed": {
        "job": catalogue_local_job,
        "op": "create_catalogue_record",
        "sensor_name": "catalogue_chain_sensor",
    },
    "verified": {
        "job": encoding_celery_job,
        "op": "encode_proxy_mp4",
        "sensor_name": "encoding_chain_sensor",
        "statuses": ("verified",),
    },
    "encoded": {
        "job": cleanup_local_job,
        "op": "check_and_delete_source",
        "sensor_name": "cleanup_chain_sensor",
    },
    "complete": {
        "job": metadata_update_local_job,
        "op": "update_cid_metadata",
        "sensor_name": "metadata_update_chain_sensor",
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
        # Cursor format: {file_id: last_attempt_epoch_seconds, ...}
        cursor: dict[int, int] = {}
        if context.cursor:
            try:
                raw = json.loads(context.cursor)
                if isinstance(raw, dict):
                    cursor = {int(k): int(v) for k, v in raw.items()}
                elif isinstance(raw, list):
                    # Upgrade old set-style cursor: mark as long-expired so retry fires immediately
                    cursor = {int(v): 0 for v in raw}
            except (json.JSONDecodeError, TypeError, ValueError):
                cursor = {}

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

        current_ids = {row[0] for row in rows}
        now = int(time.time())

        # Drop file_ids that changed status since last tick
        stale = {fid for fid in cursor if fid not in current_ids}
        for fid in stale:
            del cursor[fid]

        new_requests = []
        for file_id, file_path in rows:
            last_attempt = cursor.get(file_id)
            if last_attempt is not None and (now - last_attempt) < RETRY_INTERVAL_SECONDS:
                continue

            context.log.info(
                f"{sensor_name}: launching {op_name} for file_id={file_id} "
                f"({Path(file_path).name})"
                + (f" (retry, {now - last_attempt}s since last attempt)" if last_attempt else "")
            )
            new_requests.append(
                RunRequest(
                    run_key=f"{op_name}-{file_id}-{now}",
                    run_config={
                        "ops": {
                            op_name: {
                                "config": {"file_path": file_path}
                            }
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
cleanup_chain_sensor = _make_status_sensor("encoded", STATUS_FIELD_QUERY["encoded"])
metadata_update_chain_sensor = _make_status_sensor("complete", STATUS_FIELD_QUERY["complete"])
