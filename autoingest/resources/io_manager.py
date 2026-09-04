import hmac
import hashlib
import os
import pickle

import psycopg2
from dagster import IOManager, InputContext, OutputContext, io_manager


IO_MANAGER_RETENTION_MONTHS = 4

# ADD HMAC KEY HERE
# Set AUTOINGEST_IO_HMAC_KEY in /etc/environment on ALL servers
# (Dagster daemon + all Celery workers) before deploying this change.
# Generate a key with: python3 -c "import secrets; print(secrets.token_hex(32))"
_IO_HMAC_KEY: bytes | None = None


def _get_hmac_key() -> bytes:
    global _IO_HMAC_KEY
    if _IO_HMAC_KEY is None:
        key = os.environ.get("AUTOINGEST_IO_HMAC_KEY", "")
        if not key:
            raise RuntimeError(
                "AUTOINGEST_IO_HMAC_KEY environment variable is not set. "
                "Set it in /etc/environment on all servers before deploying."
            )
        _IO_HMAC_KEY = key.encode("utf-8")
    return _IO_HMAC_KEY


def _sign(payload: bytes) -> bytes:
    return hmac.new(_get_hmac_key(), payload, hashlib.sha256).digest()


def _verify(payload: bytes, signature: bytes) -> bool:
    expected = _sign(payload)
    return hmac.compare_digest(expected, signature)


class PostgresIOManager(IOManager):
    def __init__(self, db):
        self._db = db

    def handle_output(self, context: OutputContext, obj: object) -> None:
        run_id = context.run_id
        step_key = context.step_key
        output_name = context.name
        pickled = pickle.dumps(obj)
        signature = _sign(pickled)

        # Embed signature + pickled data in single BYTEA column
        combined = signature + pickled

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
                    (run_id, step_key, output_name, psycopg2.Binary(combined)),
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

        combined = bytes(row[0])

        # Legacy rows (written before this change) are shorter than 32 bytes
        # or have an invalid HMAC prefix. Fall back to unpickling the entire blob.
        if len(combined) <= 32:
            return pickle.loads(combined)

        signature = combined[:32]
        payload = combined[32:]

        if not _verify(payload, signature):
            # Likely legacy data without signature prefix — fall back
            return pickle.loads(combined)

        return pickle.loads(payload)


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
