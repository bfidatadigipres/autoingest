import json
import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest

from autoingest.resources.utils import (
    accepted_file_type,
    check_filename,
    check_part_whole,
    get_object_number,
    sort_ext,
    check_storage,
    move_file,
    create_md5_65536,
    create_xxhash_65536,
    get_buckets,
    get_buckets_blob,
    get_current_api,
    probe_metadata,
)


class TestAcceptedFileType:
    def test_known_extensions(self):
        cases = [
            ("avi", "avi"),
            ("mxf", "mxf, 50i, imp"),
            ("mp4", "mp4"),
            ("wav", "wav"),
            ("xml", "xml, imp"),
            ("srt", "srt"),
            ("pdf", "pdf"),
        ]
        for ext, expected in cases:
            assert accepted_file_type(ext) == expected

    def test_case_insensitive(self):
        assert accepted_file_type("MXF") == "mxf, 50i, imp"
        assert accepted_file_type("WAV") == "wav"

    def test_unknown_extension(self):
        assert accepted_file_type("xyz") is None


class TestCheckFilename:
    def test_valid_bfi_filename(self):
        assert check_filename("N_12345_01of01.mxf", screencraft=False) is True

    def test_invalid_prefix(self):
        assert check_filename("X_12345_01of01.mxf", screencraft=False) is False

    def test_screencraft_bypasses_prefix_check_but_still_checks_segments(self):
        assert check_filename("test_file_name.mxf", screencraft=True) is True

    def test_special_chars_rejected(self):
        assert check_filename("N_12345_01of01!.mxf", screencraft=False) is False
        assert check_filename("N_12345_01of01 .mxf", screencraft=False) is False

    def test_too_many_underscore_segments(self):
        assert check_filename("N_12_34_56_78_90.mxf", screencraft=False) is False

    def test_too_few_underscore_segments(self):
        assert check_filename("N12345.mxf", screencraft=False) is False

    def test_rejected_extension(self):
        assert check_filename("N_12345_01of01.exe", screencraft=False) is False

    def test_multiple_dots_rejected(self):
        assert check_filename("N_12345_01of01.test.mxf", screencraft=False) is False

    def test_prefix_only_accepted_prefixes(self):
        for px in ["N", "C", "PD", "SPD", "PBS", "PBM", "PBL", "SCR", "CA"]:
            assert check_filename(f"{px}_12345_01of01.mxf", screencraft=False) is True


class TestCheckPartWhole:
    def test_valid_multipart(self):
        assert check_part_whole("N-12345_01of02.mxf") == (1, 2)
        assert check_part_whole("C_999_02of04.mov") == (2, 4)

    def test_single_part(self):
        assert check_part_whole("N-12345_01of01.mxf") == (1, 1)

    def test_no_match(self):
        assert check_part_whole("N-12345.mxf") == (None, None)

    def test_part_larger_than_whole(self):
        assert check_part_whole("N-12345_03of02.mxf") == (None, None)

    def test_length_mismatch(self):
        assert check_part_whole("N-12345_1of02.mxf") == (None, None)

    def test_four_digit_parts(self):
        assert check_part_whole("N-12345_1234of5678.mxf") == (1234, 5678)


class TestGetObjectNumber:
    def test_typical_filename_uses_dash_joiner(self):
        assert get_object_number("N-12345_01of01.mxf") == "N-12345"

    def test_multiple_segments_joined_with_dash(self):
        assert get_object_number("C_999_02of04.mov") == "C-999"

    def test_underscores_are_joined_with_dash(self):
        assert get_object_number("N_12345_01of01.mxf") == "N-12345"


class TestSortExt:
    def test_video_types(self):
        for ext in ["mxf", "mkv", "mov", "mp4"]:
            assert sort_ext(ext) == "video"

    def test_image_types(self):
        for ext in ["jpg", "jpeg", "tif", "png"]:
            assert sort_ext(ext) == "image"

    def test_audio_types(self):
        for ext in ["wav", "flac", "mp3"]:
            assert sort_ext(ext) == "audio"

    def test_application_types(self):
        for ext in ["pdf", "xml", "srt", "csv", "docx"]:
            assert sort_ext(ext) == "application"

    def test_unknown_returns_none(self):
        assert sort_ext("xyz") is None

    def test_case_insensitive(self):
        assert sort_ext("MXF") == "video"
        assert sort_ext("JPG") == "image"
        assert sort_ext("PDF") == "application"


class TestCheckStorage:
    def test_returns_false_when_all_storage_off(self, tmp_path):
        storage_file = tmp_path / "storage_control.json"
        storage_file.write_text(json.dumps({"all_storage_on": False}))
        with patch("autoingest.resources.utils.STORAGE_JSON", str(storage_file)):
            assert check_storage("/some/path") is False

    def test_returns_path_specific_value(self, tmp_path):
        storage_file = tmp_path / "storage_control.json"
        storage_file.write_text(
            json.dumps({"all_storage_on": True, "/ingest": True, "/other": False})
        )
        with patch("autoingest.resources.utils.STORAGE_JSON", str(storage_file)):
            assert check_storage("/ingest/file.mxf") is True
            assert check_storage("/other/file.wav") is False

    def test_returns_storage_not_found(self, tmp_path):
        storage_file = tmp_path / "storage_control.json"
        storage_file.write_text(
            json.dumps({"all_storage_on": True, "/ingest": True})
        )
        with patch("autoingest.resources.utils.STORAGE_JSON", str(storage_file)):
            result = check_storage("/unknown/file.mxf")
            assert result == "Storage not found"


class TestMoveFile:
    def test_move_success(self, tmp_path):
        src = tmp_path / "source.txt"
        dst = tmp_path / "dest" / "source.txt"
        dst.parent.mkdir()
        src.write_text("data")
        result = move_file(str(src), str(dst))
        assert result[0] is True
        assert dst.exists()
        assert not src.exists()

    def test_move_source_not_found(self, tmp_path):
        result = move_file(str(tmp_path / "nonexistent.txt"), str(tmp_path / "dest.txt"))
        assert result is None


class TestCreateMd5:
    def test_known_content(self, tmp_path):
        content = b"A" * 65536 * 3 + b"B" * 1000
        f = tmp_path / "test.bin"
        f.write_bytes(content)
        expected = hashlib.md5(content).hexdigest()
        assert create_md5_65536(str(f)) == expected

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        expected = hashlib.md5(b"").hexdigest()
        assert create_md5_65536(str(f)) == expected

    def test_file_not_found(self, tmp_path):
        assert create_md5_65536(str(tmp_path / "nope.bin")) is None


class TestCreateXxhash:
    def test_known_content(self, tmp_path):
        import xxhash
        content = b"Test data for xxhash" * 5000
        f = tmp_path / "test.xx"
        f.write_bytes(content)
        expected = xxhash.xxh32(content).hexdigest()
        assert create_xxhash_65536(str(f)) == expected

    def test_file_not_found(self, tmp_path):
        assert create_xxhash_65536(str(tmp_path / "nope.bin")) is None


class TestGetBuckets:
    def test_bfi_preservation(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            bucket, blist = get_buckets("bfi", blob_accepted=False)
        assert bucket == "preservation0"
        assert "preservation0" in blist
        assert "imagen" in blist

    def test_bfi_blob(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            bucket, blist = get_buckets("bfi", blob_accepted=True)
        assert bucket == "preservationblobbing0"

    def test_netflix(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            bucket, blist = get_buckets("netflix", blob_accepted=False)
        assert bucket == "netflix0"

    def test_amazon(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            bucket, blist = get_buckets("amazon", blob_accepted=False)
        assert bucket == "amazon0"


class TestGetBucketsBlob:
    def test_bfi_blob(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            assert get_buckets_blob("bfi") == "preservationblobbing0"

    def test_netflix_blob(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            assert get_buckets_blob("netflix") == "netflixblobbing"

    def test_amazon_blob(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            assert get_buckets_blob("amazon") == "amazonblobbing"

    def test_unknown_collection(self, tmp_bucket_json):
        with patch("autoingest.resources.utils.DPI_BUCKETS", tmp_bucket_json):
            assert get_buckets_blob("disney") == ""


class TestGetCurrentApi:
    def test_returns_env_var_from_control_json(self, tmp_path, monkeypatch):
        control = tmp_path / "downtime_control.json"
        control.write_text(json.dumps({"current_api": "MY_API_URL"}))
        monkeypatch.setenv("MY_API_URL", "http://real-api.test")
        with patch("autoingest.resources.utils.CONTROL_JSON", str(control)):
            assert get_current_api() == "http://real-api.test"

    def test_returns_none_when_file_missing(self, tmp_path):
        with patch("autoingest.resources.utils.CONTROL_JSON", str(tmp_path / "nope.json")):
            assert get_current_api() is None

    def test_returns_none_when_no_api_key(self, tmp_path):
        control = tmp_path / "downtime_control.json"
        control.write_text(json.dumps({"current_api": ""}))
        with patch("autoingest.resources.utils.CONTROL_JSON", str(control)):
            assert get_current_api() is None


class TestProbeMetadata:
    @patch("autoingest.resources.utils.ffmpeg.probe")
    def test_returns_duration_tag(self, mock_probe):
        mock_probe.return_value = {
            "streams": [
                {"codec_type": "video", "tags": {"DURATION": "00:01:30.000000"}}
            ]
        }
        result = probe_metadata("duration", "video", "/fake/path.mxf")
        assert result == "00:01:30.000000"

    @patch("autoingest.resources.utils.ffmpeg.probe")
    def test_returns_stream_field(self, mock_probe):
        mock_probe.return_value = {
            "streams": [
                {"codec_type": "video", "codec_name": "h264"}
            ]
        }
        result = probe_metadata("codec_name", "video", "/fake/path.mxf")
        assert result == "h264"
