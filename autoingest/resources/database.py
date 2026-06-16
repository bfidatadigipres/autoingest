import os
import json
from pathlib import Path
import psycopg2
from contextlib import contextmanager
from dagster import resource, InitResourceContext


ALLOWED_FIELDS = {
    "id", "file_name", "file_path", "extension", "file_size",
    "checksum_md5", "file_status", "tape_verified",
    "proxy_created", "source_deletion", "created_at", "source",
    "error_message", "proxy_video_path", "proxy_size",
    "proxy_image_path", "proxy_thumb_path",
    "checksum_time_sec", "encode_time_sec", "image_time_sec",
    "verify_time_sec", "total_ingest_time_sec",
}


class WorkflowDatabase:
    def __init__(self, host, port, username, password, db_name):
        self._conn_params = {
            "host": host,
            "port": port,
            "user": username,
            "password": password,
            "dbname": db_name,
        }

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(**self._conn_params)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_field_argument(self, filename, field):
        if field not in ALLOWED_FIELDS:
            raise ValueError(f"Field '{field}' is not in the allowed fields list")
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {field} FROM app.file_catalogue WHERE file_name = %s",
                    (filename,),
                )
                return cur.fetchone()

    def lookup_file_details(self, filename):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM app.file_catalogue WHERE file_name LIKE %s",
                    (filename,),
                )
                return cur.fetchone()

    def create_file_record(self, file_data: list):
        fname, fpath, ftype, fsize = file_data
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.file_catalogue
                        (file_name, file_path, extension, file_size)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (fname, fpath, ftype, fsize),
                )
                return cur.fetchone()[0]

    def update_file_status(self, file_id: int, **fields):
        if not fields:
            return
        for key in fields:
            if key not in ALLOWED_FIELDS:
                raise ValueError(f"Field '{key}' is not allowed for update")
        set_clause = ", ".join(f"{k} = %s" for k in fields)
        values = list(fields.values()) + [file_id]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE app.file_catalogue SET {set_clause} WHERE id = %s",
                    values,
                )

    def get_pending_tape_files(self, max_bytes: int):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, file_path, file_size, checksum_md5, source
                    FROM app.file_catalogue
                    WHERE file_status = 'File cleared for ingest'
                    ORDER BY created_at ASC
                    """
                )
                rows = cur.fetchall()
                batch = []
                total = 0
                for row in rows:
                    if total + row[2] > max_bytes:
                        break
                    batch.append(row)
                    total += row[2]
                return batch

    def check_all_stages_complete(self, file_id: int) -> bool:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tape_verified, proxy_created
                    FROM app.file_catalogue WHERE id = %s
                    """,
                    (file_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return False
                return row[0] is True and row[1] is True

    def try_claim_file(self, file_name: str, file_path: str) -> int | None:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, file_status FROM app.file_catalogue "
                    "WHERE file_name = %s FOR UPDATE",
                    (file_name,),
                )
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        "INSERT INTO app.file_catalogue "
                        "(file_name, file_path, file_status) "
                        "VALUES (%s, %s, 'processing') RETURNING id",
                        (file_name, file_path),
                    )
                    return cur.fetchone()[0]
                if existing[1] == "No Status":
                    cur.execute(
                        "UPDATE app.file_catalogue "
                        "SET file_status = 'processing', updated_at = NOW() "
                        "WHERE id = %s",
                        (existing[0],),
                    )
                    return existing[0]
                return None

    def upsert_file_record(self, file_data: dict) -> tuple[int, str]:
        file_name = file_data.get("file_name")
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, file_status FROM app.file_catalogue "
                    "WHERE file_name = %s ORDER BY created_at DESC LIMIT 1",
                    (file_name,),
                )
                existing = cur.fetchone()

        if existing is None:
            record_id = self.create_file_record([
                file_name,
                str(file_data.get("file_path", "")),
                file_data.get("extension", ""),
                file_data.get("file_size", 0),
            ])
            return record_id, "insert"

        existing_id, existing_status = existing
        if existing_status in ("No Status", "Failed assessment"):
            self._update_retry_record(existing_id, file_data)
            return existing_id, "update"
        if existing_status in ("File cleared for ingest",):
            return existing_id, "update"

        return existing_id, "skip"

    def _update_retry_record(self, record_id: int, file_data: dict):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE app.file_catalogue
                    SET file_path = %s,
                        extension = %s,
                        file_size = %s,
                        file_status = 'No Status',
                        error_message = NULL,
                        do_ingest = 'UNKNOWN',
                        incomplete_scan = %s,
                        screencraft_arch = %s,
                        mime_type = %s,
                        source = %s,
                        tape_verified = NULL,
                        proxy_created = NULL,
                        source_deletion = NULL,
                        validated = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                """, (
                    str(file_data.get("file_path", "")),
                    file_data.get("extension", ""),
                    file_data.get("file_size", 0),
                    file_data.get("incomplete_scan", "UNKNOWN"),
                    file_data.get("screencraft_arch", "UNKNOWN"),
                    file_data.get("mime_type", ""),
                    file_data.get("source", ""),
                    record_id,
                ))

    def delete_file_record(self, file_id: int):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app.file_catalogue WHERE id = %s",
                    (file_id,),
                )

    def get_non_retryable_cursor_files(self, file_paths: set[str]) -> set[str]:
        filenames = [Path(p).name for p in file_paths]
        if not filenames:
            return set()
        placeholders = ", ".join(["%s"] * len(filenames))
        query = (
            f"SELECT file_name FROM app.file_catalogue "
            f"WHERE file_name IN ({placeholders}) "
            f"AND file_status != 'No Status'"
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, filenames)
                done = {r[0] for r in cur.fetchall()}
        return {p for p in file_paths if Path(p).name in done}

    def record_pipeline_event(
        self,
        run_id: str,
        job_name: str,
        op_name: str,
        event_type: str,
        status: str,
        metadata: dict | None = None,
        message: str | None = None,
    ):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.pipeline_events
                        (run_id, job_name, op_name, event_type,
                         status, message, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        job_name,
                        op_name,
                        event_type,
                        status,
                        message,
                        json.dumps(metadata) if metadata else None,
                    ),
                )

    def get_aggregate_metrics(
        self,
        op_name: str | None = None,
        event_type: str | None = None,
        days: int = 30,
    ) -> list[dict]:
        query = """
            SELECT
                op_name,
                event_type,
                COUNT(*) AS event_count,
                COUNT(*) FILTER (WHERE status = 'success') AS success_count,
                COUNT(*) FILTER (WHERE status = 'failure') AS failure_count,
                AVG((metadata->>'duration_sec')::float) AS avg_duration_sec,
                MIN((metadata->>'duration_sec')::float) AS min_duration_sec,
                MAX((metadata->>'duration_sec')::float) AS max_duration_sec,
                AVG((metadata->>'file_size')::bigint) AS avg_file_size
            FROM app.pipeline_events
            WHERE created_at >= NOW() - INTERVAL %s
        """
        params: list = [f"{days} days"]
        if op_name:
            query += " AND op_name = %s"
            params.append(op_name)
        if event_type:
            query += " AND event_type = %s"
            params.append(event_type)
        query += " GROUP BY op_name, event_type ORDER BY op_name, event_type"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]


@resource
def workflow_database(context: InitResourceContext) -> WorkflowDatabase:
    return WorkflowDatabase(
        host=os.environ["WORKFLOW_PG_HOST"],
        port=int(os.environ.get("WORKFLOW_PG_PORT", "5432")),
        username=os.environ["WORKFLOW_PG_USERNAME"],
        password=os.environ["WORKFLOW_PG_PASSWORD"],
        db_name=os.environ["WORKFLOW_PG_DB"],
    )