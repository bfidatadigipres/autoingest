import os
from pathlib import Path
from dagster import op, OpExecutionContext

from autoingest.resources.io_manager import cleanup_io_manager_store


@op(required_resource_keys={"workflow_db"})
def sweep_completed_files(context: OpExecutionContext) -> None:
    db = context.resources.workflow_db
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, file_path FROM app.file_catalogue
                WHERE tape_verified = TRUE
                  AND proxy_created = TRUE
                  AND (source_deletion IS NULL)
                  AND file_status = 'metadata_updated'
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

    try:
        deleted = cleanup_io_manager_store(db)
        context.log.info(f"IO manager store cleanup: {deleted} rows removed")
    except Exception as exc:
        context.log.warning(f"IO manager store cleanup skipped: {exc}")
