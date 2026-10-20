from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock

from autoingest.resources.database import WorkflowDatabase


def _mock_db(rows):
    db = WorkflowDatabase("host", 5432, "user", "password", "database")
    cur = MagicMock()
    cur.fetchall.return_value = rows

    cursor_ctx = MagicMock()
    cursor_ctx.__enter__ = MagicMock(return_value=cur)
    cursor_ctx.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor_ctx)

    @contextmanager
    def fake_connection():
        yield conn

    db.get_connection = fake_connection
    return db, cur


class TestBatchLookupFileStates:
    def test_returns_status_error_and_updated_at(self):
        updated_at = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
        db, cur = _mock_db(
            [
                (
                    "/ingest/bfi/N12345_02of03.mxf",
                    "No Status",
                    "Waiting for previous part to ingest",
                    updated_at,
                )
            ]
        )

        states = db.batch_lookup_file_states({"/ingest/bfi/N12345_02of03.mxf"})

        assert states["/ingest/bfi/N12345_02of03.mxf"] == (
            "No Status",
            "Waiting for previous part to ingest",
            updated_at,
        )
        query = cur.execute.call_args[0][0]
        assert "error_message" in query
        assert "updated_at" in query

    def test_duplicate_paths_keep_oldest_row(self):
        first = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
        second = datetime(2026, 10, 20, 13, 0, tzinfo=UTC)
        db, _ = _mock_db(
            [
                ("/ingest/bfi/N12345_02of03.mxf", "No Status", None, first),
                (
                    "/ingest/bfi/N12345_02of03.mxf",
                    "Duplicate filename",
                    "duplicate",
                    second,
                ),
            ]
        )

        states = db.batch_lookup_file_states({"/ingest/bfi/N12345_02of03.mxf"})

        assert states["/ingest/bfi/N12345_02of03.mxf"] == (
            "No Status",
            None,
            first,
        )

    def test_empty_paths_returns_empty_dict(self):
        db, cur = _mock_db([])

        assert db.batch_lookup_file_states(set()) == {}
        cur.execute.assert_not_called()
