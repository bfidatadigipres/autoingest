import json
import time
from pathlib import Path
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from autoingest.jobs.ingest_jobs import ingest_celery_job, catalogue_local_job
from autoingest.jobs.validation_jobs import encoding_celery_job, cleanup_local_job


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
    },
    "encoded": {
        "job": cleanup_local_job,
        "op": "check_and_delete_source",
        "sensor_name": "cleanup_chain_sensor",
    },
}


def _make_status_sensor(status: str, conf: dict):
    job = conf["job"]
    op_name = conf["op"]
    sensor_name = conf["sensor_name"]

    @sensor(
        job=job,
        name=sensor_name,
        minimum_interval_seconds=30,
        default_status=DefaultSensorStatus.RUNNING,
        required_resource_keys={"workflow_db"},
    )
    def _sensor_fn(context: SensorEvaluationContext) -> list[RunRequest]:
        triggered_ids = set()
        if context.cursor:
            try:
                triggered_ids = set(json.loads(context.cursor))
            except (json.JSONDecodeError, TypeError):
                triggered_ids = set()

        db = context.resources.workflow_db
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, file_path FROM app.file_catalogue "
                    "WHERE file_status = %s ORDER BY created_at ASC",
                    (status,),
                )
                rows = cur.fetchall()

        current_ids = {row[0] for row in rows}

        # Drop file_ids that changed status since last tick (removes stale entries)
        triggered_ids = triggered_ids & current_ids

        new_requests = []
        for file_id, file_path in rows:
            if file_id in triggered_ids:
                continue

            context.log.info(
                f"{sensor_name}: launching {op_name} for file_id={file_id} "
                f"({Path(file_path).name})"
            )
            new_requests.append(
                RunRequest(
                    run_key=f"{op_name}-{file_id}-{int(time.time())}",
                    run_config={
                        "ops": {
                            op_name: {
                                "config": {"file_path": file_path}
                            }
                        }
                    },
                )
            )
            triggered_ids.add(file_id)

        if new_requests:
            context.log.info(f"{sensor_name}: launching {len(new_requests)} run(s)")

        context.update_cursor(json.dumps(list(triggered_ids)))
        return new_requests

    return _sensor_fn


ingest_chain_sensor = _make_status_sensor("assessed", STATUS_FIELD_QUERY["assessed"])
catalogue_chain_sensor = _make_status_sensor("checksummed", STATUS_FIELD_QUERY["checksummed"])
encoding_chain_sensor = _make_status_sensor("verified", STATUS_FIELD_QUERY["verified"])
cleanup_chain_sensor = _make_status_sensor("encoded", STATUS_FIELD_QUERY["encoded"])
