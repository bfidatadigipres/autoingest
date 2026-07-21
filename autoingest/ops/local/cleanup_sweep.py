import os
import time
from pathlib import Path
from dagster import op, OpExecutionContext

from autoingest.resources.io_manager import cleanup_io_manager_store


@op(required_resource_keys={"workflow_db"})
def sweep_completed_files(context: OpExecutionContext) -> None:
    tic = time.perf_counter()
    db = context.resources.workflow_db

    search_conditions = {
        "tape_verified": True,
        "proxy_created": True,
        "source_deletion": "IS NULL",
        "file_status": "encoding_complete",
        "extra": "source file must exist on disk",
    }

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, file_path FROM app.file_catalogue
                WHERE tape_verified = TRUE
                  AND proxy_created = TRUE
                  AND (source_deletion IS NULL)
                  AND file_status = 'encoding_complete'
                """
            )
            rows = cur.fetchall()

    total_found = len(rows)
    deleted = 0
    skipped = 0
    deletion_details = []

    _record_event(
        db, context,
        status="success",
        metadata={
            "phase": "search_criteria",
            "conditions": search_conditions,
            "files_matched": total_found,
            "preview": f"Sweep search: {total_found} files matched deletion criteria",
        },
        message=(
            "SQL search: tape_verified=TRUE AND proxy_created=TRUE "
            "AND source_deletion IS NULL AND file_status='encoding_complete'; "
            f"matched {total_found} rows"
        ),
    )

    for file_id, filepath in rows:
        fname = Path(filepath).name
        ftic = time.perf_counter()
        source = Path(filepath)
        if source.exists():
            try:
                file_size = os.path.getsize(filepath)
            except OSError:
                file_size = 0
            context.log.info(f"Sweep deleting source: {filepath}")
            source.unlink()
            db.update_file_status(file_id, file_status="complete", source_deletion=True)
            deleted += 1
            duration = round(time.perf_counter() - ftic, 3)
            deletion_details.append({
                "file_name": fname,
                "file_path": str(filepath),
                "file_size": file_size,
                "outcome": "deleted",
                "duration_sec": duration,
            })
            context.log.info(f"Sweep deleted: {fname} ({duration}s)")
        else:
            skipped += 1
            deletion_details.append({
                "file_name": fname,
                "file_path": str(filepath),
                "outcome": "skipped (source not found on disk)",
            })
            context.log.info(f"Sweep skipped: {fname} (source not found)")

    io_cleaned = 0
    io_error = None
    try:
        io_cleaned = cleanup_io_manager_store(db)
        context.log.info(f"IO manager store cleanup: {io_cleaned} rows removed")
    except Exception as exc:
        io_error = str(exc)
        context.log.warning(f"IO manager store cleanup skipped: {exc}")

    total_duration = round(time.perf_counter() - tic, 3)

    _record_event(
        db, context,
        status="success",
        metadata={
            "phase": "sweep_summary",
            "files_found": total_found,
            "files_deleted": deleted,
            "files_skipped": skipped,
            "io_store_cleaned": io_cleaned,
            "io_store_error": io_error,
            "duration_sec": total_duration,
            "deletions": deletion_details,
            "preview": (
                f"Sweep complete: {deleted} deleted, "
                f"{skipped} skipped out of {total_found} matched; "
                f"{total_duration}s"
            ),
        },
        message=(
            f"Sweep finished: {deleted} files deleted, "
            f"{skipped} skipped, {total_found} matched, "
            f"IO store: {io_cleaned} purged"
            + (f" (IO error: {io_error})" if io_error else "")
        ),
    )

    context.log.info(
        f"Sweep complete: {deleted} deleted, {skipped} skipped "
        f"({total_duration}s)"
    )


def _record_event(db, context, status, metadata, message=None):
    try:
        db.record_pipeline_event(
            run_id=context.run_id,
            job_name=context.job_name,
            op_name="sweep_completed_files",
            event_type="op_completed",
            status=status,
            metadata=metadata,
            message=message,
        )
    except Exception:
        pass
