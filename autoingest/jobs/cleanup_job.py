from dagster import job, op


@op(tags={"dagster-celery/queue": "default"})
def sweep_completed_files(context, workflow_db):
    with workflow_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, filepath FROM file_catalogue
                WHERE tape_verified = TRUE
                  AND proxy_created = TRUE
                  AND source_deleted IS NOT TRUE
                  AND status != 'complete'
                """
            )
            rows = cur.fetchall()

    import os
    from pathlib import Path

    for file_id, filepath in rows:
        source = Path(filepath)
        if source.exists():
            context.log.info(f"Sweep deleting source: {filepath}")
            source.unlink()
        workflow_db.update_file_status(
            file_id, status="complete", source_deleted=True
        )

    context.log.info(f"Sweep complete: {len(rows)} files cleaned up")


@job(resource_defs={"workflow_db": "workflow_db"})
def cleanup_job():
    sweep_completed_files()
