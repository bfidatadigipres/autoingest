import json
import os
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

ds3_stub = MagicMock()
ds3_stub.createClientFromEnv.return_value = MagicMock()
sys.modules["ds3"] = ds3_stub

_tmp = tempfile.mkdtemp()
os.environ.setdefault("LOG_PATH", _tmp)
os.environ.setdefault("CID_API_URL", "http://fake-api.test")
os.environ.setdefault("DPI_BUCKET", os.path.join(_tmp, "buckets.json"))
os.environ.setdefault("JSON_END_POINT", "http://fake-bp-endpoint.test")
with open(os.path.join(_tmp, "downtime_control.json"), "w") as f:
    json.dump({"current_api": "CID_API_URL"}, f)
with open(os.environ["DPI_BUCKET"], "w") as f:
    json.dump({"bfi0": True}, f)

from autoingest.resources.utils import WAITING_FOR_PREVIOUS_PART_MSG
from autoingest.sensors.watch_folder import (
    _launch_budget,
    _prioritise_multipart,
    _retry_token,
    _storage_key,
)

BASE = "/mnt/qnap_09/autoingest/ingest/bfi"


def _candidate(part: int) -> str:
    return f"{BASE}/OBJ_{part:02d}of65.mxf"


def _db(status=None, do_ingest="TRUE"):
    db = MagicMock()
    db.lookup_file_details.return_value = (
        (1, "prev.mxf", status, "prev.mxf", "", "BFI", do_ingest) if status else None
    )
    return db


def test_returns_only_the_next_part():
    result = _prioritise_multipart(
        _db("File cleared for ingest"),
        [_candidate(part) for part in range(2, 66)],
        {},
        set(),
    )
    assert result == [_candidate(2)]


def test_advances_when_previous_is_in_flight():
    result = _prioritise_multipart(
        _db("processing"),
        [_candidate(part) for part in range(3, 66)],
        {},
        set(),
    )
    assert result == [_candidate(3)]


def test_blocks_when_previous_failed():
    result = _prioritise_multipart(
        _db("Failed assessment", "FALSE"),
        [_candidate(part) for part in range(3, 66)],
        {},
        set(),
    )
    assert result == []


def test_new_series_starts_at_part_one():
    result = _prioritise_multipart(
        _db(),
        [_candidate(part) for part in range(1, 4)],
        {},
        {_candidate(1)},
    )
    assert result == [_candidate(1)]


def test_unknown_previous_is_allowed_once_for_new_file():
    assert _prioritise_multipart(_db(), [_candidate(2)], {}, set()) == [_candidate(2)]


def test_unknown_previous_retry_waits_four_hours():
    recent = datetime.now(UTC) - timedelta(hours=1)
    result = _prioritise_multipart(
        _db(),
        [_candidate(2)],
        {_candidate(2): ("No Status", "Waiting", recent)},
        set(),
    )
    assert result == []


def test_unknown_previous_retry_allowed_after_four_hours():
    old = datetime.now(UTC) - timedelta(hours=5)
    result = _prioritise_multipart(
        _db(),
        [_candidate(2)],
        {_candidate(2): ("No Status", "Waiting", old)},
        set(),
    )
    assert result == [_candidate(2)]


def test_incomplete_scan_is_not_ordered():
    path = "/mnt/qnap_09/autoingest/ingest/bfi/incomplete_scan/OBJ_02of65.mxf"
    assert _prioritise_multipart(_db(), [path], {}, set()) == [path]


def test_storage_key_standard_path():
    assert _storage_key("/mnt/qnap_09/autoingest/ingest/bfi/f.mxf") == "qnap_09"


def test_storage_key_isolates_adjacent_shares():
    scanning = "/mnt/bp_nas/film/scanning/autoingest/ingest/bfi/f.mxf"
    operations = "/mnt/bp_nas/film/operations/autoingest/ingest/bfi/f.mxf"
    assert _storage_key(scanning) == "bp_nas/film/scanning"
    assert _storage_key(operations) == "bp_nas/film/operations"


def test_storage_key_ingest_only_path():
    path = "/mnt/bp_nas/film/scanning/ingest/bfi/f.mxf"
    assert _storage_key(path) == "bp_nas/film/scanning"


def test_storage_key_falls_back_to_mount():
    assert _storage_key("/mnt/qnap_09/foo/f.mxf") == "qnap_09"


def test_launch_budget_below_soft_limit():
    assert _launch_budget(149) == (60, 45, 15)


def test_launch_budget_at_soft_limit():
    assert _launch_budget(150) == (5, 4, 1)


def test_launch_budget_at_hard_limit():
    assert _launch_budget(300) == (5, 4, 1)


def test_launch_budget_above_hard_limit():
    assert _launch_budget(301) is None


def test_retry_token_for_waiting_part_uses_updated_at():
    updated = datetime.now(UTC)
    state = ("No Status", WAITING_FOR_PREVIOUS_PART_MSG, updated)
    assert _retry_token(_candidate(2), {_candidate(2): state}) == int(
        updated.timestamp()
    )


def test_retry_token_for_failed_assessment_is_none():
    state = ("Failed assessment", "FFprobe failed", datetime.now(UTC))
    assert _retry_token(_candidate(2), {_candidate(2): state}) is None


def test_retry_token_for_other_no_status_is_none():
    state = ("No Status", "", datetime.now(UTC))
    assert _retry_token(_candidate(2), {_candidate(2): state}) is None
