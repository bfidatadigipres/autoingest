import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

ds3_stub = MagicMock()
ds3_stub.createClientFromEnv.return_value = MagicMock()
sys.modules["ds3"] = ds3_stub

_tmp = tempfile.mkdtemp()
os.environ.setdefault("LOG_PATH", _tmp)
os.environ.setdefault("CID_API_URL", "http://fake-api.test")
os.environ.setdefault("DPI_BUCKET", os.path.join(_tmp, "buckets.json"))
os.environ.setdefault("JSON_END_POINT", "http://fake-bp-endpoint.test")
_control = os.path.join(_tmp, "downtime_control.json")
with open(_control, "w") as f:
    json.dump({"current_api": "CID_API_URL"}, f)
with open(os.environ["DPI_BUCKET"], "w") as f:
    json.dump({"bfi0": True}, f)

import autoingest.ops.local.verification as vmod
from autoingest.ops.local.verification import (
    retrieve_json_data,
    check_for_failed_file,
    json_check,
)


class TestRetrieveJsonData:
    def test_returns_matching_file(self, tmp_path):
        d = tmp_path / "bp_logs"
        d.mkdir(parents=True)
        (d / "job_1234.json").write_text("{}")
        (d / "other.json").write_text("{}")
        with patch.object(vmod, "JSON_PATH", str(d)):
            result = retrieve_json_data("1234")
            assert result is not None
            assert "1234" in result

    def test_returns_none_when_no_match(self, tmp_path):
        d = tmp_path / "bp_logs"
        d.mkdir(parents=True)
        with patch.object(vmod, "JSON_PATH", str(d)):
            assert retrieve_json_data("9999") is None


class TestJsonCheck:
    def test_returns_objects_not_persisted(self, tmp_path):
        data = {
            "Notification": {
                "Event": {"ObjectsNotPersisted": [{"Name": "file1.mxf"}, {"Name": "file2.mxf"}]}
            }
        }
        f = tmp_path / "notify.json"
        f.write_text(json.dumps(data))
        result = json_check(str(f))
        assert result == [{"Name": "file1.mxf"}, {"Name": "file2.mxf"}]

    def test_no_notification_returns_none(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text(json.dumps({"Other": "data"}))
        result = json_check(str(f))
        assert result is None

    def test_no_objects_not_persisted_returns_none(self, tmp_path):
        data = {"Notification": {"Event": {"SomeOtherField": "value"}}}
        f = tmp_path / "notify.json"
        f.write_text(json.dumps(data))
        result = json_check(str(f))
        assert result is None

    def test_notification_not_dict_returns_none(self, tmp_path):
        data = {"Notification": "just a string"}
        f = tmp_path / "bad.json"
        f.write_text(json.dumps(data))
        result = json_check(str(f))
        assert result is None


class TestCheckForFailedFile:
    def test_file_found_in_failed_list(self, tmp_path):
        data = {
            "Notification": {
                "Event": {
                    "ObjectsNotPersisted": [{"Name": "failed.mxf"}, {"Name": "also_failed.mxf"}]
                }
            }
        }
        f = tmp_path / "failed.json"
        f.write_text(json.dumps(data))
        assert check_for_failed_file("failed.mxf", str(f)) is True

    def test_file_not_in_failed_list(self, tmp_path):
        data = {
            "Notification": {
                "Event": {"ObjectsNotPersisted": [{"Name": "other.mxf"}]}
            }
        }
        f = tmp_path / "failed.json"
        f.write_text(json.dumps(data))
        assert check_for_failed_file("good.mxf", str(f)) is False

    def test_empty_failed_list_returns_false(self, tmp_path):
        data = {"Notification": {"Event": {"ObjectsNotPersisted": []}}}
        f = tmp_path / "failed.json"
        f.write_text(json.dumps(data))
        assert check_for_failed_file("any.mxf", str(f)) is False
