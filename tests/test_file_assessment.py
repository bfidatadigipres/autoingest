import json
import os
import sys
import tempfile
from unittest.mock import MagicMock

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

from autoingest.ops.local.file_assessment import (
    get_data_from_path,
    process_image_archive,
    check_for_multipart,
)


class TestGetDataFromPath:
    def test_netflix_path(self):
        donor, scan, scr = get_data_from_path("/ingest/netflix/some/file.mxf")
        assert donor == "Netflix"
        assert scan is False

    def test_amazon_path(self):
        donor, scan, scr = get_data_from_path("/ingest/amazon/file.mov")
        assert donor == "Amazon"

    def test_disney_path(self):
        donor, scan, scr = get_data_from_path("/ingest/disney/file.mxf")
        assert donor == "Disney"

    def test_bfi_default(self):
        donor, scan, scr = get_data_from_path("/some/other/path.mxf")
        assert donor == "BFI"

    def test_incomplete_scan_flag(self):
        donor, scan, scr = get_data_from_path("/ingest/bfi/incomplete_scan/file.mxf")
        assert scan is True

    def test_screencraft_flag(self):
        donor, scan, scr = get_data_from_path(
            "/Screencraft/path/proxy/image/archive/file.dpx"
        )
        assert scr is True

    def test_flags_independent(self):
        donor, scan, scr = get_data_from_path(
            "/Screencraft/incomplete_scan/proxy/image/archive/file.dpx"
        )
        assert scan is True
        assert scr is True


class TestProcessImageArchive:
    def test_standard_archive(self):
        obj, part, whole = process_image_archive("OBJ_1234_01of02.dpx")
        assert obj == "OBJ-1234"
        assert part == 1
        assert whole == 2

    def test_hyphen_in_name_returns_none(self):
        assert process_image_archive("OBJ-1234_01of02.dpx") == (None, None, None)

    def test_part_whole_mismatch_length(self):
        assert process_image_archive("OBJ_1234_1of02.dpx") == (None, None, None)

    def test_part_too_long(self):
        assert process_image_archive(
            "OBJ_1234_12345of12345.dpx"
        ) == (None, None, None)

    def test_part_zero(self):
        assert process_image_archive("OBJ_1234_00of02.dpx") == (None, None, None)

    def test_part_greater_than_whole(self):
        assert process_image_archive("OBJ_1234_03of02.dpx") == (None, None, None)


class TestCheckForMultipart:
    def test_single_part_returns_true(self):
        assert check_for_multipart("N12345_01of01.mxf", 1, 1) is True

    def test_part_one_returns_true(self):
        assert check_for_multipart("N12345_01of02.mxf", 1, 2) is True

    def test_returns_previous_part(self):
        result = check_for_multipart("N12345_02of02.mxf", 2, 2)
        assert result == "N12345_01of02"

    def test_three_part_series(self):
        result = check_for_multipart("N12345_03of03.mxf", 3, 3)
        assert result == "N12345_02of03"

    def test_four_segment_filename(self):
        result = check_for_multipart("N_123_AB_02of04.mov", 2, 4)
        assert result == "N_123_AB_01of04"

    def test_different_padding_length(self):
        result = check_for_multipart("N1_0002of0010.mxf", 2, 10)
        assert result == "N1_0001of0010"

    def test_second_part_of_two(self):
        result = check_for_multipart("CA_123456_02of02.dpx", 2, 2)
        assert result == "CA_123456_01of02"
