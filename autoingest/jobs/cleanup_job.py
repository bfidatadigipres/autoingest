"""
Unsure if this one is needed, as validation job triggers deletion
"""

import os
from pathlib import Path
from dagster import job, op, OpExecutionContext


@op(required_resource_keys={"workflow_db"})
def sweep_completed_files(context: OpExecutionContext):
    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, file_path FROM app.file_catalogue
                WHERE tape_verified = TRUE
                  AND proxy_created = TRUE
                  AND (source_deletion IS NULL)
                  AND file_status != 'complete'
                """
            )
            rows = cur.fetchall()

    for file_id, filepath in rows:
        source = Path(filepath)
        if source.exists():
            context.log.info(f"Sweep deleting source: {filepath}")
            source.unlink()
        db.update_file_status(file_id, file_status="complete", source_deletion=True)

    context.log.info(f"Sweep complete: {len(rows)} files cleaned up")


@job
def cleanup_job():
    sweep_completed_files()
