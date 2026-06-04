import os
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
from types import SimpleNamespace
from dagster import resource, InitResourceContext


@resource
def workflow_database(context: InitResourceContext):
    conn_params = {
        "host": os.environ["WORKFLOW_PG_HOST"],
        "port": int(os.environ["WORKFLOW_PG_PORT"]),
        "user": os.environ["WORKFLOW_PG_USERNAME"],
        "password": os.environ["WORKFLOW_PG_PASSWORD"],
        "dbname": os.environ["WORKFLOW_PG_DB"],
    }

    @contextmanager
    def get_connection():
        conn = psycopg2.connect(**conn_params)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def lookup_file_details(filename):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM file_catalogue WHERE file_name LIKE %s",
                    (filename,),
                )
                return cur.fetchone()

    def create_file_record(file_data):
        fname, fpath, ftype, fsize = file_data
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO file_catalogue
                        (file_name, file_path, extension, file_size)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (fname, fpath, ftype, fsize),
                )
                return cur.fetchone()[0]

    def update_file_status(file_id, **fields):
        if not fields:
            return
        set_parts = []
        values = []
        for key, value in fields.items():
            set_parts.append(
                sql.SQL("{} = %s").format(sql.Identifier(key))
            )
            values.append(value)
        values.append(file_id)
        query = sql.SQL("UPDATE file_catalogue SET {} WHERE id = %s").format(
            sql.SQL(", ").join(set_parts)
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)

    def get_pending_tape_files(max_bytes):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, file_path, file_size, checksum_md5, source
                    FROM file_catalogue
                    WHERE file_status = 'File cleared for ingest'
                    ORDER BY created_at ASC
                    """
                )
                rows = cur.fetchall()
                batch, total = [], 0
                for row in rows:
                    if total + row[2] > max_bytes:
                        break
                    batch.append(row)
                    total += row[2]
                return batch

    def check_all_stages_complete(file_id):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tape_verified, proxy_created
                    FROM file_catalogue WHERE id = %s
                    """,
                    (file_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return False
                return row[0] is True and row[1] is True

    return SimpleNamespace(
        get_connection=get_connection,
        lookup_file_details=lookup_file_details,
        create_file_record=create_file_record,
        update_file_status=update_file_status,
        get_pending_tape_files=get_pending_tape_files,
        check_all_stages_complete=check_all_stages_complete,
    )