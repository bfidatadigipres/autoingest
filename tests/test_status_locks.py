"""
Tier B tests for status-locking guards and rollback helpers.

Tests the in-progress claim → expected-status check → rollback pattern
for every sensor-triggered op that has been protected.
"""

from unittest.mock import MagicMock, patch

from dagster import build_op_context

from autoingest.ops.celery.checksum import (
    _set_encoding_status as _set_checksum_status,
    _rollback_checksum_status,
)
from autoingest.ops.local.db_documentation import (
    _set_cataloguing_status,
    _rollback_cataloguing_status,
)
from autoingest.ops.local.source_deletion import (
    _set_deleting_status,
    _rollback_deleting_status,
)
from autoingest.ops.local.cid_metadata_update import _set_updating_status
from autoingest.ops.celery.proxy_video import (
    _set_encoding_status,
    _rollback_encoding_status,
)
from autoingest.ops.celery.proxy_images import (
    _set_images_status,
    _rollback_images_status,
)


# ---------------------------------------------------------------------------
# Shared mock helpers
# ---------------------------------------------------------------------------

def _mock_db():
    """Return a MagicMock WorkflowDatabase with a usable connection/cursor."""
    db = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    conn.__enter__ = MagicMock(return_value=cur)
    conn.__exit__ = MagicMock(return_value=False)
    db.get_connection = MagicMock(return_value=conn)

    def cursor_ctx():
        ctx_m = MagicMock()
        ctx_m.__enter__ = MagicMock(return_value=cur)
        ctx_m.__exit__ = MagicMock(return_value=False)
        return ctx_m

    conn.cursor = cursor_ctx
    return db


# ---------------------------------------------------------------------------
# Tier B: helper function tests (pure — no op context needed)
# ---------------------------------------------------------------------------

class TestSetStatusHelpers:
    """Each helper should execute the correct SQL to set its in-progress status."""

    def test_set_checksum_status(self):
        db = _mock_db()
        _set_checksum_status(db, 42)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'generating_checksum'" in sql
        assert "error_message = NULL" in sql

    def test_set_cataloguing_status(self):
        db = _mock_db()
        _set_cataloguing_status(db, 7)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'cataloguing'" in sql

    def test_set_deleting_status(self):
        db = _mock_db()
        _set_deleting_status(db, 3)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'deleting_source'" in sql

    def test_set_updating_status(self):
        db = _mock_db()
        _set_updating_status(db, 9)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'updating_cid'" in sql

    def test_set_encoding_status(self):
        db = _mock_db()
        _set_encoding_status(db, 55)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'encoding'" in sql

    def test_set_images_status(self):
        db = _mock_db()
        _set_images_status(db, 88)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'generating_images'" in sql


class TestRollbackHelpers:
    """Each rollback helper should reset to the previous expected status."""

    def test_rollback_checksum_status(self):
        db = _mock_db()
        _rollback_checksum_status(db, 42)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'assessed'" in sql

    def test_rollback_cataloguing_status(self):
        db = _mock_db()
        _rollback_cataloguing_status(db, 7)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'checksummed'" in sql

    def test_rollback_deleting_status(self):
        db = _mock_db()
        _rollback_deleting_status(db, 3)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'encoding_complete'" in sql

    def test_rollback_encoding_status(self):
        db = _mock_db()
        _rollback_encoding_status(db, 55)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'verified'" in sql

    def test_rollback_images_status(self):
        db = _mock_db()
        _rollback_images_status(db, 88)
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        sql = cur.execute.call_args[0][0]
        assert "file_status = 'verified'" in sql


# ---------------------------------------------------------------------------
# Tier B: guard logic — verify each op skips when status is wrong
# ---------------------------------------------------------------------------

class TestEncodeProxyMp4Guard:
    """encode_proxy_mp4 should skip on 'encoding' and non-'verified'."""

    def test_skips_when_already_encoding(self):
        from autoingest.ops.celery.proxy_video import encode_proxy_mp4

        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1, "encoding", "video", "bfi", "202606", "abc123")

        ctx = build_op_context(
            resources={"workflow_db": db, "encoding_config": MagicMock()},
            op_config={"file_path": "/fake/path.mkv"},
        )
        result = encode_proxy_mp4(ctx)
        assert result.value is None

    def test_skips_when_not_verified(self):
        from autoingest.ops.celery.proxy_video import encode_proxy_mp4

        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1, "encoding_complete", "video", "bfi", "202606", "abc123")

        ctx = build_op_context(
            resources={"workflow_db": db, "encoding_config": MagicMock()},
            op_config={"file_path": "/fake/path.mkv"},
        )
        result = encode_proxy_mp4(ctx)
        assert result.value is None


class TestGenerateImagesGuard:
    """generate_images should skip on 'generating_images' and non-'encoded'."""

    def test_skips_when_already_generating_images(self):
        from autoingest.ops.celery.proxy_images import generate_images

        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ("generating_images",)

        ctx = build_op_context(
            resources={"workflow_db": db, "encoding_config": MagicMock()},
        )
        file_info = {
            "file_id": 1, "file_path": "/fake/path.mkv",
            "proxy_video_path": "/fake/proxy.mp4",
            "mime_type": "video", "source": "bfi",
        }
        result = generate_images(ctx, file_info)
        assert result.value is not None

    def test_skips_when_not_encoded(self):
        from autoingest.ops.celery.proxy_images import generate_images

        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ("verified",)

        ctx = build_op_context(
            resources={"workflow_db": db, "encoding_config": MagicMock()},
        )
        file_info = {
            "file_id": 1, "file_path": "/fake/path.mkv",
            "proxy_video_path": "/fake/proxy.mp4",
            "mime_type": "video", "source": "bfi",
        }
        result = generate_images(ctx, file_info)
        assert "Skipped" in str(result.metadata.get("preview", ""))


class TestGenerateChecksumGuard:
    """generate_checksum should skip on 'generating_checksum'."""

    def test_skips_when_already_checksumming(self):
        from autoingest.ops.celery.checksum import generate_checksum

        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1, "generating_checksum", "TRUE", 12345, "/fake/path.mkv")

        ctx = build_op_context(
            resources={"workflow_db": db},
            op_config={"file_path": "/fake/path.mkv"},
        )
        result = generate_checksum(ctx)
        assert result.value == {}


class TestCreateCatalogueRecordGuard:
    """create_catalogue_record should skip on 'cataloguing' and non-'checksummed'."""

    _cols = [
        "id", "file_name", "file_path", "extension", "file_size",
        "error_message", "incomplete_scan", "screencraft_arch",
        "checksum_md5", "checksum_date", "checksum_xxh",
        "mdata_full_text", "mdata_text", "mdata_ebucore",
        "mdata_pbcore", "mdata_full_xml", "mdata_full_json",
        "whole", "part", "do_ingest", "ffprobe_exit",
        "bp_bucket", "bucket_list", "mime_type",
        "cid_file_type", "cid_item_priref", "cid_ob_num",
        "source", "put_type", "autoingest_path",
        "file_status",
    ]

    def _build_row(self, file_status):
        vals = [1, "test.mkv", "/fake/path.mkv", "mkv", 12345]
        vals += [""] * 25  # pad remaining columns before file_status
        vals.append(file_status)
        return tuple(vals)

    def _setup_ctx(self, file_status):
        from autoingest.ops.local.db_documentation import create_catalogue_record
        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = self._build_row(file_status)
        ctx = build_op_context(
            resources={"workflow_db": db},
            op_config={"file_path": "/fake/path.mkv"},
        )
        return create_catalogue_record, ctx

    def test_skips_when_already_cataloguing(self):
        op, ctx = self._setup_ctx("cataloguing")
        result = op(ctx)
        preview = result.metadata.get("preview")
        assert preview is not None
        assert "cataloguing" in str(preview)

    def test_skips_when_not_checksummed(self):
        op, ctx = self._setup_ctx("complete")
        result = op(ctx)
        preview = result.metadata.get("preview")
        assert preview is not None
        assert "Skipped" in str(preview)


class TestCheckAndDeleteSourceGuard:
    """check_and_delete_source should skip on 'deleting_source' and non-'encoding_complete'."""

    def _build_row(self, file_status):
        return (1, file_status, "/fake/path.mkv", "abc", "/f/p.mp4",
                "/f/l.jpg", "/f/t.jpg", "123", "abcd", "2026-01-01")

    def _setup_ctx(self, file_status):
        from autoingest.ops.local.source_deletion import check_and_delete_source
        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = self._build_row(file_status)
        ctx = build_op_context(
            resources={"workflow_db": db},
            op_config={"file_path": "/fake/path.mkv"},
        )
        return check_and_delete_source, ctx

    def test_skips_when_already_deleting(self):
        op, ctx = self._setup_ctx("deleting_source")
        result = op(ctx)
        assert "Already deleting source" in str(result.metadata.get("preview", ""))

    def test_skips_when_not_encoding_complete(self):
        op, ctx = self._setup_ctx("verified")
        result = op(ctx)
        assert "Skipped" in str(result.metadata.get("preview", ""))


class TestUpdateCidMetadataGuard:
    """update_cid_metadata should skip on 'updating_cid' and non-'complete'."""

    def _setup_ctx(self, file_status):
        from autoingest.ops.local.cid_metadata_update import update_cid_metadata
        db = _mock_db()
        cur = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1, "test.mkv", file_status, "video", "123",
                                     None, None, None, None, None, None, None)
        ctx = build_op_context(
            resources={"workflow_db": db},
            op_config={"file_path": "/fake/path.mkv"},
        )
        return update_cid_metadata, ctx

    def test_skips_when_already_updating(self):
        op, ctx = self._setup_ctx("updating_cid")
        result = op(ctx)
        assert "Already updating CID" in str(result.metadata.get("preview", ""))

    def test_skips_when_not_complete(self):
        op, ctx = self._setup_ctx("encoding_complete")
        result = op(ctx)
        assert "Skipped" in str(result.metadata.get("preview", ""))
