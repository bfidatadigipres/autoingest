import pickle

import psycopg2
from dagster import IOManager, InputContext, OutputContext, io_manager


IO_MANAGER_RETENTION_MONTHS = 4


class PostgresIOManager(IOManager):
    def __init__(self, db):
        self._db = db

    def handle_output(self, context: OutputContext, obj: object) -> None:
        run_id = context.run_id
        step_key = context.step_key
        output_name = context.name
        pickled = pickle.dumps(obj)

        with self._db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.io_manager_store
                        (run_id, step_key, output_name, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id, step_key, output_name)
                    DO UPDATE SET value = EXCLUDED.value, created_at = NOW()
                    """,
                    (run_id, step_key, output_name, psycopg2.Binary(pickled)),
                )

    def load_input(self, context: InputContext) -> object:
        upstream = context.upstream_output
        run_id = upstream.run_id
        step_key = upstream.step_key
        output_name = upstream.name

        with self._db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT value FROM app.io_manager_store
                    WHERE run_id = %s AND step_key = %s AND output_name = %s
                    """,
                    (run_id, step_key, output_name),
                )
                row = cur.fetchone()

        if row is None:
            raise FileNotFoundError(
                f"No stored value in io_manager_store for "
                f"run={run_id}, step={step_key}, output={output_name}"
            )

        return pickle.loads(bytes(row[0]))


@io_manager(required_resource_keys={"workflow_db"})
def postgres_io_manager(context):
    return PostgresIOManager(context.resources.workflow_db)


def cleanup_io_manager_store(db: object) -> int:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM app.io_manager_store "
                "WHERE created_at < NOW() - INTERVAL %s",
                (f"{IO_MANAGER_RETENTION_MONTHS} months",),
            )
            deleted = cur.rowcount
    return deleted
