"""
Tier A tests for extract_metadata — field-mapping logic from Mediainfo JSON.

Verifies the loop-inversion fix and json.loads fix from recent sessions
by testing the mapping between Mediainfo track fields and DB columns.
"""

import json
from unittest.mock import MagicMock, patch

from dagster import build_op_context

from autoingest.ops.local.extract_metadata import extract_metadata


# --- Known-good Mediainfo JSON fixture ---
MEDIAINFO_JSON_STR = json.dumps({
    "media": {
        "track": [
            {
                "@type": "General",
                "Format": "Matroska",
                "Video_Codec_List": "FFV1",
                "Audio_Codec_List": "PCM",
                "Encoded_Application": "Lavf62.1.100",
                "Audio_Format_List": "PCM",
                "FrameRate_String": "25.000",
                "Audio_Channels_Total": "2",
                "AudioCount": "1",
                "VideoCount": "1",
            },
            {
                "@type": "Video",
                "Height_String": "1080",
                "Width_String": "1920",
                "ColorSpace": "YUV",
                "BitDepth": "10",
                "Duration": "3600.000",
            },
        ]
    }
})

IMAGE_JSON_STR = json.dumps({
    "media": {
        "track": [
            {
                "@type": "General",
                "Format": "TIFF",
                "ImageCount": "1",
            },
        ]
    }
})


def _mock_db():
    """Return a MagicMock that simulates the workflow_db resource."""
    db = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    conn.__enter__ = MagicMock(return_value=cur)
    conn.__exit__ = MagicMock(return_value=False)
    db.get_connection = MagicMock(return_value=conn)

    def cursor_ctx():
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=cur)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx

    conn.cursor = cursor_ctx
    return db


class TestExtractMetadataFieldMapping:
    """Verify that all DB columns are correctly populated from Mediainfo JSON."""

    def test_all_general_fields_populated(self):
        """All 9 mdata_general fields should be set from known JSON."""
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {
            "do_ingest": "TRUE",
            "file_path": "/fake/path.mkv",
            "file_name": "test.mkv",
            "mime_type": "video",
            "mdata_full_json": MEDIAINFO_JSON_STR,
        }

        with patch("autoingest.ops.local.extract_metadata.utils.make_metadata",
                   return_value=MEDIAINFO_JSON_STR):
            extract_metadata(ctx, file_info)

        assert file_info.get("file_fmt") == "Matroska"
        assert file_info.get("video_codec") == "FFV1"
        assert file_info.get("audio_codec") == "PCM"
        assert file_info.get("writing_library") == "Lavf62.1.100"
        assert file_info.get("audio_format") == "PCM"
        assert file_info.get("framerate") == "25.000"
        assert file_info.get("audio_ch_total") == "2"
        assert file_info.get("audio_count") == "1"
        assert file_info.get("video_count") == "1"

    def test_all_video_fields_populated(self):
        """All 5 mdata_video fields should be set from known JSON."""
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {
            "do_ingest": "TRUE",
            "file_path": "/fake/path.mkv",
            "file_name": "test.mkv",
            "mime_type": "video",
            "mdata_full_json": MEDIAINFO_JSON_STR,
        }

        with patch("autoingest.ops.local.extract_metadata.utils.make_metadata",
                   return_value=MEDIAINFO_JSON_STR):
            extract_metadata(ctx, file_info)

        assert file_info.get("height") == "1080"
        assert file_info.get("width") == "1920"
        assert file_info.get("colorspace") == "YUV"
        assert file_info.get("bitdepth") == "10"
        assert file_info.get("video_duration") == "3600.000"

    def test_no_reset_bug_multiple_general_fields(self):
        """Regression: the old loop inversion bug reset previously matched fields.
        All fields found in the JSON must survive — not just the last one."""
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {
            "do_ingest": "TRUE",
            "file_path": "/fake/path.mkv",
            "file_name": "test.mkv",
            "mime_type": "video",
            "mdata_full_json": MEDIAINFO_JSON_STR,
        }

        with patch("autoingest.ops.local.extract_metadata.utils.make_metadata",
                   return_value=MEDIAINFO_JSON_STR):
            extract_metadata(ctx, file_info)

        assert file_info.get("file_fmt") == "Matroska"
        assert file_info.get("video_codec") == "FFV1"
        assert file_info.get("audio_codec") == "PCM"
        assert file_info.get("framerate") == "25.000"
        assert file_info.get("video_count") == "1"

    def test_isinstance_passes_with_json_string(self):
        """Regression: mdata_full_json was a string; the old isinstance check
        returned False and silently skipped all extraction. After the json.loads
        fix, extraction should proceed when a valid JSON string is supplied."""
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {
            "do_ingest": "TRUE",
            "file_path": "/fake/path.mkv",
            "file_name": "test.mkv",
            "mime_type": "video",
            "mdata_full_json": MEDIAINFO_JSON_STR,
        }

        with patch("autoingest.ops.local.extract_metadata.utils.make_metadata",
                   return_value=MEDIAINFO_JSON_STR):
            extract_metadata(ctx, file_info)

        assert file_info.get("file_fmt") == "Matroska"

    def test_isinstance_falls_back_on_invalid_json(self):
        """If mdata_full_json is not valid JSON, the isinstance guard should
        catch it and leave metadata fields unset (without crashing)."""
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {
            "do_ingest": "TRUE",
            "file_path": "/fake/path.mkv",
            "file_name": "test.mkv",
            "mime_type": "video",
            "mdata_full_json": "not-valid-json{{{",
        }

        with patch("autoingest.ops.local.extract_metadata.utils.make_metadata",
                   return_value="not-valid-json{{{"):
            extract_metadata(ctx, file_info)

        assert file_info.get("file_fmt") is None

    def test_fields_missing_from_json_are_empty_string(self):
        """When a mapped field is not present in the Mediainfo JSON, it should
        be set to "" rather than left unset."""
        json_no_video = json.dumps({
            "media": {
                "track": [
                    {"@type": "General", "Format": "Matroska"},
                ]
            }
        })
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {
            "do_ingest": "TRUE",
            "file_path": "/fake/path.mkv",
            "file_name": "test.mkv",
            "mime_type": "video",
            "mdata_full_json": json_no_video,
        }

        with patch("autoingest.ops.local.extract_metadata.utils.make_metadata",
                   return_value=json_no_video):
            extract_metadata(ctx, file_info)

        assert file_info.get("file_fmt") == "Matroska"
        assert file_info.get("video_codec") == ""
        assert file_info.get("audio_codec") == ""

    def test_skips_when_do_ingest_not_true(self):
        """If do_ingest is not TRUE, the op should return immediately without
        touching metadata."""
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {"do_ingest": "FALSE", "file_path": "/fake/path.mkv"}

        result = extract_metadata(ctx, file_info)
        assert result.value == file_info
        assert file_info.get("file_fmt") is None

    def test_image_file_adds_exif_data(self):
        """For image files, mdata_exif should be populated."""
        db = _mock_db()
        ctx = build_op_context(resources={"workflow_db": db})

        file_info = {
            "do_ingest": "TRUE",
            "file_path": "/fake/path.tif",
            "file_name": "test.tif",
            "mime_type": "image",
            "mdata_full_json": IMAGE_JSON_STR,
        }

        with patch("autoingest.ops.local.extract_metadata.utils.make_metadata",
                   return_value=IMAGE_JSON_STR):
            with patch("autoingest.ops.local.extract_metadata.utils.exif_data",
                       return_value=["EXIF line 1", "EXIF line 2"]):
                extract_metadata(ctx, file_info)

        assert "EXIF line 1\nEXIF line 2" == file_info.get("mdata_exif")
