import os
import psycopg2
from contextlib import contextmanager
from dagster import resource, InitResourceContext


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

    def lookup_file_details(self, filename):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM file_fields
                    WHERE filename LIKE %s
                    """,
                    (filename,),
                )
                return cur.fetchone()

    def create_file_record(self, file_data: list):
        fname, fpath, ftype, fsize = file_data
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO file_catalogue
                        (file_name, file_path, extension, file_size)
                    VALUES
                        (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (fname, fpath, ftype, fsize,)
                )
                return cur.fetchone()[0]

    def update_file_status(self, file_id: int, **fields):
        set_clause = ", ".join(f"{k} = %({k})s" for k in fields)
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE file_catalogue SET {set_clause} WHERE id = %(file_id,)s",
                    file_id,
                )

    def get_pending_tape_files(self, max_bytes: int):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, filepath, filesize, checksum_md5
                    FROM file_catalogue
                    WHERE status = 'metadata_complete'
                    ORDER BY created_at ASC
                    """,
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
                    FROM file_catalogue
                    WHERE id = %s
                    """,
                    (file_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return False
                return row[0] is True and row[1] is True


@resource
def workflow_database(context: InitResourceContext) -> WorkflowDatabase:
    return WorkflowDatabase(
        host=os.environ["WORKFLOW_PG_HOST"],
        port=int(os.environ["WORKFLOW_PG_PORT"]),
        username=os.environ["WORKFLOW_PG_USERNAME"],
        password=os.environ["WORKFLOW_PG_PASSWORD"],
        db_name=os.environ["WORKFLOW_PG_DB"],
    )
