import os
import subprocess
import time
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytz

import pytest

from autoingest.resources.proxy_utils import (
    build_audio_args,
    select_video_filter,
    _safe_int,
    adjust_seconds,
    _retrieve_blackspaces,
    _check_seconds,
    check_mod_time,
    validate_mp4_moov,
    VIDEO_FILTERS,
)


class TestBuildAudioArgs:
    def test_mixed_dict(self):
        result = build_audio_args("Audio", None, {"DL": 0, "DR": 1}, False, False)
        assert "-map" in result
        assert "0:a:0" in result
        assert "0:a:1" in result

    def test_fl_fr(self):
        result = build_audio_args("Audio", None, None, fl_fr=True, twelve_chnl=False)
        assert "-ac" in result
        assert result[result.index("-ac") + 1] == "2"

    def test_twelve_channel(self):
        result = build_audio_args("Audio", None, None, fl_fr=False, twelve_chnl=True)
        assert "-af" in result
        assert any("pan=stereo" in arg for arg in result)

    def test_default_audio(self):
        result = build_audio_args("Audio", "0", None, False, False)
        assert "-map" in result
        assert "0:a?" in result

    def test_no_audio(self):
        result = build_audio_args(None, None, None, False, False)
        assert result == ["-map", "0:a?", "-c:a", "aac", "-dn"]

    def test_no_default_with_audio(self):
        result = build_audio_args("Audio", None, None, False, False)
        assert result == ["-map", "0:a?", "-c:a", "aac", "-dn"]


class TestSelectVideoFilter:
    def test_subsd_169(self):
        result = select_video_filter(400, 720, 1.85, "16:9", "1.0")
        assert result == ["-vf", VIDEO_FILTERS["upscale_sd_width"]]

    def test_ntsc_486_169(self):
        result = select_video_filter(486, 720, 1.78, "16:9", "1.0")
        assert "-vf" in result

    def test_ntsc_486_43(self):
        result = select_video_filter(486, 720, 1.33, "4:3", "1.0")
        assert "-vf" in result

    def test_hd_720p(self):
        result = select_video_filter(720, 1280, 1.78, "16:9", "1.0")
        assert "-vf" in result

    def test_fhd_above_720(self):
        result = select_video_filter(1080, 1920, 1.78, "16:9", "1.0")
        assert "-vf" in result

    def test_no_match_returns_empty(self):
        result = select_video_filter(600, 800, 0.5, "99:99", "99.9")
        assert result == []


class TestSafeInt:
    def test_valid_integer_string(self):
        assert _safe_int("42", 0) == 42

    def test_none_returns_default(self):
        assert _safe_int(None, 10) == 10

    def test_empty_string_returns_default(self):
        assert _safe_int("", 5) == 5

    def test_already_int(self):
        assert _safe_int(42, 0) == 42


class TestRetrieveBlackspaces:
    def test_parses_blackdetect_lines(self):
        data = (
            "[blackdetect @ 0x1] black_start:10 black_end:15 black_duration:5\n"
            "[blackdetect @ 0x1] black_start:30 black_end:35 black_duration:5\n"
        )
        result = _retrieve_blackspaces(data)
        assert result == ["10 - 16", "30 - 36"]

    def test_no_blackdetect_returns_empty(self):
        data = "[some log line]\n[another line]\n"
        assert _retrieve_blackspaces(data) == []

    def test_empty_string(self):
        assert _retrieve_blackspaces("") == []


class TestCheckSeconds:
    def test_seconds_within_blackspace(self):
        blackspace = ["10 - 15", "30 - 35"]
        assert _check_seconds(blackspace, 12) is True

    def test_seconds_outside_blackspace(self):
        blackspace = ["10 - 15", "30 - 35"]
        assert _check_seconds(blackspace, 20) is None

    def test_seconds_at_lower_boundary(self):
        blackspace = ["10 - 15"]
        assert _check_seconds(blackspace, 9) is True

    def test_seconds_at_upper_boundary_exclusive(self):
        blackspace = ["10 - 15"]
        assert _check_seconds(blackspace, 16) is None


class TestAdjustSeconds:
    def test_no_blackspace_returns_three_candidates(self):
        result = adjust_seconds(100, "")
        assert result == [25.0, 50.0, 75.0]

    def test_avoids_blackspace(self):
        data = "[blackdetect @ 0x1] black_start:20 black_end:30 black_duration:10\n"
        result = adjust_seconds(100, data)
        assert len(result) == 3
        for val in result:
            assert val < 20.0 or val > 30.0

    def test_short_duration(self):
        result = adjust_seconds(4, "")
        assert result == [1.0, 2.0, 3.0]


class TestCheckModTime:
    def test_recent_mod_time_returns_true(self, tmp_path):
        f = tmp_path / "test.mp4"
        f.write_text("")
        ts = time.time() - 10800
        os.utime(str(f), (ts, ts))
        assert check_mod_time(str(f)) is True

    def test_old_mod_time_returns_false(self, tmp_path):
        f = tmp_path / "test.mp4"
        f.write_text("")
        ts = time.time() - 21600
        os.utime(str(f), (ts, ts))
        assert check_mod_time(str(f)) is False


class TestValidateMp4Moov:
    def test_valid_mp4_returns_true(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "mov,mp4,m4a,3gp,3g2,mj2\n"
        with patch("autoingest.resources.proxy_utils.subprocess.run", return_value=mock_result):
            ok, err = validate_mp4_moov("/tmp/test.mp4")
        assert ok is True
        assert err == ""

    def test_missing_moov_returns_false(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "[mov,mp4,m4a,3gp,3g2,mj2 @ 0x1] moov atom not found\n/tmp/test.mp4: Invalid data found when processing input"
        with patch("autoingest.resources.proxy_utils.subprocess.run", return_value=mock_result):
            ok, err = validate_mp4_moov("/tmp/test.mp4")
        assert ok is False
        assert "moov atom not found" in err

    def test_empty_stdout_returns_false(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        with patch("autoingest.resources.proxy_utils.subprocess.run", return_value=mock_result):
            ok, err = validate_mp4_moov("/tmp/test.mp4")
        assert ok is False
        assert "MOOV atom not found" in err
