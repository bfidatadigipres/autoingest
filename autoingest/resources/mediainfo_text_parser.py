"""
MediaInfo TEXT FULL → JSON parser.

Converts the output of `mediainfo -f --Output=TEXT` (or `--Output=TEXT` without -f)
into a dict structure compatible with MediaInfo's native JSON output, so that
build_metadata_xml_from_db() in cid_metadata_update.py can consume it.

Usage:
    from autoingest.resources.mediainfo_text_parser import text_full_to_json
    result = text_full_to_json(text_full_string, "filename.ts")
"""

import re
from typing import Any

# ── Track header patterns ──────────────────────────────────────────────
_TRACK_RE = re.compile(
    r"^(?P<type>General|Video|Audio(?:\s*#\s*\d+)?|Text(?:\s*#\s*\d+)?|Menu)\s*$",
    re.IGNORECASE,
)

# ── Key→value separator ────────────────────────────────────────────────
_KV_SEP = " : "

# ── Comprehensive TEXT FULL key → JSON key mapping ────────────────────
# Covers every field referenced in FIELDS (cid_metadata_update.py) and
# the mdata_general/mdata_video dicts (extract_metadata.py).
KEY_MAP: dict[str, str] = {
    # ── General track special mappings ─────────────────────────────
    "Codecs Video": "Video_Codec_List",
    "Audio codecs": "Audio_Codec_List",
    "Text codecs": "Text_Codec_List",
    "Menu codecs": "Menu_Codec_List",
    "Count of video streams": "VideoCount",
    "Count of audio streams": "AudioCount",
    "Count of text streams": "TextCount",
    "Count of menu streams": "MenuCount",
    "Kind of stream": "StreamKind",
    "Count of stream of this kind": "StreamCount",
    "Stream identifier": "StreamKindID",
    "Commercial name": "Format_Commercial",
    "Format/Extensions usually used": "Format_Extensions",
    "Internet media type": "InternetMediaType",
    "File size": "FileSize",
    "Overall bit rate mode": "OverallBitRate_Mode",
    "Overall bit rate": "OverallBitRate",
    "Frame rate": "FrameRate",
    "Frame count": "FrameCount",
    "Stream size": "StreamSize",
    "Proportion of this stream": "StreamSize_Proportion",
    "File last modification date": "File_Modified_Date",
    "File last modification date (local)": "File_Modified_Date_Local",
    "Start time": "Duration_Start",
    "End time": "Duration_End",
    "Country": "Country",
    "Timezone": "TimeZone",
    "Complete name": "CompleteName",
    "File name extension": "FileNameExtension",
    "File name": "FileName",
    "File extension": "FileExtension",
    "Unique ID": "UniqueID",
    "Is truncated": "IsTruncated",
    "Encoded date": "Encoded_Date",
    "Writing application": "Encoded_Application",
    "Writing library": "Encoded_Library",
    "Duration": "Duration",
    "Format": "Format",
    "Format profile": "Format_Profile",
    "Format version": "Format_Version",
    "Format settings": "Format_Settings",
    "Format settings, GOP": "Format_Settings_GOP",
    "Codec ID": "CodecID",
    "Bit rate": "BitRate",
    "Bit rate mode": "BitRate_Mode",
    "Width": "Width",
    "Height": "Height",
    "Pixel aspect ratio": "PixelAspectRatio",
    "Display aspect ratio": "DisplayAspectRatio",
    "Active Format Description": "ActiveFormatDescription",
    "Active Format Description, Muxing mode": "ActiveFormatDescription_MuxingMode",
    "Bits/(Pixel*Frame)": "BitsPixel_Frame",
    "Delay": "Delay",
    "Delay, origin": "Delay_Source",
    "Color space": "ColorSpace",
    "Chroma subsampling": "ChromaSubsampling",
    "Bit depth": "BitDepth",
    "Scan type": "ScanType",
    "Scan type, store method": "ScanType_StoreMethod",
    "Scan order": "ScanOrder",
    "Color range": "colour_range",
    "Color primaries": "colour_primaries",
    "Transfer characteristics": "transfer_characteristics",
    "Matrix coefficients": "matrix_coefficients",
    "Width clean aperture": "Width_CleanAperture",
    "StreamOrder": "StreamOrder",
    "Standard": "Standard",
    "Compression mode": "Compression_Mode",
    "Channel(s)": "Channels",
    "Channel positions": "ChannelPositions",
    "Channel layout": "ChannelLayout",
    "Samples per frame": "SamplesPerFrame",
    "Sampling rate": "SamplingRate",
    "Samples count": "SamplingCount",
    "Language": "Language",
    "Format settings, Endianness": "Format_Settings_Endianness",
    "Format settings, Sign": "Format_Settings_Sign",
    "Format settings, CABAC": "Format_Settings_CABAC",
    "Format settings, Reference frames": "Format_Settings_RefFrames",
    "Service kind": "ServiceKind",
    "Time code of first frame": "TimeCode_FirstFrame",
    "Type": "Type",
    "Delay relative to video": "Video_Delay",
    # Already-underscored keys (identity mapping)
    "Video_Format_List": "Video_Format_List",
    "Video_Format_WithHint_List": "Video_Format_WithHint_List",
    "Audio_Format_List": "Audio_Format_List",
    "Audio_Format_WithHint_List": "Audio_Format_WithHint_List",
    "Audio_Language_List": "Audio_Language_List",
    "Audio_Channels_Total": "Audio_Channels_Total",
    "Text_Format_List": "Text_Format_List",
    "Text_Format_WithHint_List": "Text_Format_WithHint_List",
    "Text_Language_List": "Text_Language_List",
    "Menu_Format_List": "Menu_Format_List",
    "Menu_Format_WithHint_List": "Menu_Format_WithHint_List",
    "Menu_Language_List": "Menu_Language_List",
    "OverallBitRate_Precision_Min": "OverallBitRate_Precision_Min",
    "OverallBitRate_Precision_Max": "OverallBitRate_Precision_Max",
    "MaxSlicesCount": "MaxSlicesCount",
    "Stored_Height": "Stored_Height",
    "Sampled_Width": "Sampled_Width",
    "Sampled_Height": "Sampled_Height",
    "ID": "ID",
    "Menu ID": "MenuID",
    "Count": "Count",
}

# ── Keys that change name based on occurrence index ───────────────────
# (key, 0-based occurrence) → JSON key
OCCURRENCE_KEY_MAP: dict[tuple[str, int], str] = {
    ("Stream identifier", 0): "StreamKindID",
    ("Stream identifier", 1): "StreamKindPos",
}

# ── Duplicate-key suffix pattern (MediaInfo convention) ───────────────
# 1st → base, 2nd → _String, 3rd → _String1, 4th → _String2, ...
def _suffix_for_occurrence(occurrence: int) -> str:
    if occurrence == 0:
        return ""
    if occurrence == 1:
        return "_String"
    return f"_String{occurrence - 1}"


def _parse_tracks(text_full: str) -> list[tuple[str, str | None, list[tuple[str, str]]]]:
    """Split TEXT FULL output into tracks.

    Returns list of (track_type, typeorder_or_None, [(key, value), ...]).
    """
    tracks: list[tuple[str, str | None, list[tuple[str, str]]]] = []
    current_type: str | None = None
    current_order: str | None = None
    current_lines: list[tuple[str, str]] = []

    for raw_line in text_full.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        m = _TRACK_RE.match(line)
        if m:
            # Save previous track
            if current_type is not None:
                tracks.append((current_type, current_order, current_lines))
            current_type = m.group("type").strip()
            # Extract typeorder from "Audio #2" / "Text #1"
            order_match = re.search(r"#\s*(\d+)", current_type)
            current_order = order_match.group(1) if order_match else None
            # Normalise the type name
            current_type = re.sub(r"\s*#\s*\d+", "", current_type).strip()
            current_lines = []
            continue

        if _KV_SEP in line and current_type is not None:
            key, _, value = line.partition(_KV_SEP)
            current_lines.append((key.strip(), value.strip()))

    # Save last track
    if current_type is not None:
        tracks.append((current_type, current_order, current_lines))

    return tracks


def _resolve_key(raw_key: str, occurrence: int) -> str | None:
    """Resolve a TEXT FULL key (+ occurrence) to its JSON key.

    Returns None if the key is unknown and should go into extra.
    """
    # Special occurrence-based mapping
    occ_key = OCCURRENCE_KEY_MAP.get((raw_key, occurrence))
    if occ_key is not None:
        return occ_key

    # Direct mapping
    mapped = KEY_MAP.get(raw_key)
    if mapped is not None:
        return mapped + _suffix_for_occurrence(occurrence)

    # Unknown key → caller puts it in extra
    return None


def _parse_track(
    track_type: str, typeorder: str | None, kv_pairs: list[tuple[str, str]]
) -> dict[str, Any]:
    """Build a single track dict from parsed key-value pairs."""
    track: dict[str, Any] = {"@type": track_type}
    if typeorder is not None:
        track["@typeorder"] = typeorder

    extra: dict[str, str] = {}
    occurrence_counter: dict[str, int] = {}

    for raw_key, value in kv_pairs:
        if not value:
            continue

        occ = occurrence_counter.get(raw_key, 0)
        occurrence_counter[raw_key] = occ + 1

        json_key = _resolve_key(raw_key, occ)

        if json_key is None:
            # Unknown key → extra
            # Normalise the key for extra: CamelCase + underscores
            norm = _normalise_extra_key(raw_key, occ)
            extra[norm] = value
        else:
            # Special handling: Format_Profile split on @
            if json_key == "Format_Profile" and "@" in value:
                parts = value.split("@", 1)
                track["Format_Profile"] = parts[0].strip()
                level = parts[1].strip()
                if level:
                    track["Format_Level"] = level
            else:
                track[json_key] = value

    if extra:
        track["extra"] = extra

    return track


def _normalise_extra_key(raw_key: str, occurrence: int) -> str:
    """Normalise an unknown key for the extra dict.

    Applies MediaInfo-style CamelCase conversion and duplicate suffix.
    """
    # Remove parentheses content
    key = re.sub(r"\s*\(.*?\)", "", raw_key).strip()
    # Replace / and , with underscores
    key = key.replace("/", "_").replace(", ", "_").replace(",", "_")
    # Remove special chars
    key = re.sub(r"[*()]", "", key)
    # Collapse multiple underscores
    key = re.sub(r"_+", "_", key)
    # Split on spaces, capitalise each word, join
    words = key.split()
    if words:
        key = words[0] + "".join(w.capitalize() for w in words[1:])

    suffix = _suffix_for_occurrence(occurrence)
    return key + suffix


def text_full_to_json(text_full: str, filename: str) -> dict[str, Any]:
    """Convert MediaInfo TEXT FULL (or TEXT) output to JSON-compatible dict.

    Args:
        text_full: Raw output from `mediainfo -f --Output=TEXT` (or just TEXT).
        filename: Original filename for the @ref field.

    Returns:
        A dict matching the structure of MediaInfo's native JSON output,
        suitable for consumption by build_metadata_xml_from_db().
        Missing fields are simply absent from the output — no crash.
    """
    tracks_parsed = _parse_tracks(text_full)

    track_dicts: list[dict[str, Any]] = []
    for track_type, typeorder, kv_pairs in tracks_parsed:
        track_dicts.append(_parse_track(track_type, typeorder, kv_pairs))

    return {
        "creatingLibrary": {
            "name": "Automated conversion from the TEXT FULL - MediaInfoLib",
            "version": "unknown",
            "url": "https://mediaarea.net/MediaInfo",
        },
        "media": {
            "@ref": filename,
            "track": track_dicts,
        },
    }
