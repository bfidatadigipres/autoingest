import os
import re
import sys
from typing import Final, Optional, Union
import adlib
import utils

_BLACKDETECT = "blackdetect=d=0.05:pix_th=0.10"

VIDEO_FILTERS = {
    "crop_sd_608":        f"yadif,crop=672:572:24:32,scale=734:576:flags=lanczos,pad=768:576:-1:-1,{_BLACKDETECT}",
    "no_stretch_4x3":     f"yadif,pad=768:576:-1:-1,{_BLACKDETECT}",
    "crop_sd_4x3":        f"yadif,crop=672:572:24:2,scale=734:576:flags=lanczos,pad=768:576:-1:-1,{_BLACKDETECT}",
    "upscale_sd_width":   f"yadif,scale=1024:-1:flags=lanczos,pad=1024:576:-1:-1,{_BLACKDETECT}",
    "upscale_sd_height":  f"yadif,scale=-1:576:flags=lanczos,pad=1024:576:-1:-1,{_BLACKDETECT}",
    "scale_sd_4x3":       f"yadif,scale=768:576:flags=lanczos,{_BLACKDETECT}",
    "scale_sd_16x9":      f"yadif,scale=1024:576:flags=lanczos,{_BLACKDETECT}",
    "crop_sd_15x11":      f"yadif,crop=704:572,scale=768:576:flags=lanczos,pad=768:576:-1:-1,{_BLACKDETECT}",
    "crop_ntsc_486":      f"yadif,crop=672:480,scale=734:486:flags=lanczos,pad=768:486:-1:-1,{_BLACKDETECT}",
    "crop_ntsc_486_16x9": f"yadif,crop=672:480,scale=1024:486:flags=lanczos,{_BLACKDETECT}",
    "crop_ntsc_640x480":  f"yadif,pad=768:480:-1:-1,{_BLACKDETECT}",
    "crop_sd_16x9":       f"yadif,crop=704:572:8:2,scale=1024:576:flags=lanczos,{_BLACKDETECT}",
    "sd_downscale_4x3":   f"yadif,scale=768:576:flags=lanczos,{_BLACKDETECT}",
    "hd_16x9":            f"yadif,scale=-1:720:flags=lanczos,pad=1280:720:-1:-1,{_BLACKDETECT}",
    "hd_16x9_letterbox":  f"yadif,scale=1280:-1:flags=lanczos,pad=1280:720:-1:-1,{_BLACKDETECT}",
    "fhd_all":            f"yadif,scale=-1:1080:flags=lanczos,pad=1920:1080:-1:-1,{_BLACKDETECT}",
    "fhd_letters":        f"yadif,scale=1920:-1:flags=lanczos,pad=1920:1080:-1:-1,{_BLACKDETECT}",
}





def _build_audio_args(
    audio: Optional[str],
    default: Optional[str],
    mixed_dict: Optional[dict[str, int]],
    fl_fr: bool,
    twelve_chnl: bool,
) -> list[str]:
    """Build the audio-mapping portion of the FFmpeg command."""
    if mixed_dict:
        print(f"Mixed DL DR audio found: {mixed_dict}")
        return [
            "-map", f"0:a:{mixed_dict['DL']}",
            "-map", f"0:a:{mixed_dict['DR']}",
            "-ac", "2",
            "-c:a:0", "aac", "-ab:1", "320k", "-ar:1", "48000", "-ac:1", "2",
            "-disposition:a:0", "default",
            "-c:a:1", "aac", "-ab:2", "210k", "-ar:2", "48000", "-ac:2", "1",
            "-disposition:a:1", "0",
            "-strict", "2", "-async", "1", "-dn",
        ]
    if fl_fr:
        return ["-map", "0:a?", "-c:a", "aac", "-ac", "2", "-dn"]
    if twelve_chnl:
        return [
            "-map", "0:a?",
            "-af", "pan=stereo|c0=FL+0.707*FC|c1=FR+0.707*FC",
            "-c:a", "aac", "-b:a", "192k", "-dn",
        ]
    if default and audio:
        print(f"Default {default}, Audio {audio}")
        return [
            "-map", "0:a?", "-c:a", "aac",
            f"-disposition:a:{default}", "default", "-dn",
        ]
    return ["-map", "0:a?", "-c:a", "aac", "-dn"]


def _select_video_filter(
    height: int,
    width: int,
    aspect: float,
    dar: str,
    par: str,
) -> list[str]:
    """Pick the right -vf args based on resolution, DAR, and PAR.

    Returns ["-vf", "<filter_chain>"] or [] if nothing matched.
    """
    name = None

    # --- Sub-SD ---
    if height < 480 and aspect >= 1.778:
        name = "upscale_sd_width"
    elif height < 480 and aspect < 1.778:
        name = "upscale_sd_height"

    # --- NTSC 486-line ---
    elif height == 486 and dar == "16:9":
        name = "crop_ntsc_486_16x9"
    elif height == 486 and dar == "4:3":
        name = "crop_ntsc_486"
    elif height <= 486 and width == 640:
        name = "crop_ntsc_640x480"

    # --- PAL / SD with specific widths ---
    elif height < 576 and width == 720 and dar == "4:3":
        name = "scale_sd_4x3"
    elif height == 576 and width == 703 and dar != "16:9":
        name = "scale_sd_4x3"
    elif height == 576 and width == 703 and dar == "16:9":
        name = "scale_sd_16x9"
    elif height == 576 and width == 1024:
        name = "scale_sd_16x9"
    elif height < 576 and width > 720 and dar == "16:9":
        name = "scale_sd_16x9"
    elif height < 576 and width > 720 and dar == "4:3":
        name = "sd_downscale_4x3"

    # --- SD catch-all by DAR / PAR ---
    elif height <= 576 and dar == "16:9":
        name = "crop_sd_16x9"
    elif height <= 576 and width == 768:
        name = "no_stretch_4x3"
    elif height <= 576 and par == "1.000":
        name = "no_stretch_4x3"
    elif height <= 576 and dar == "4:3":
        name = "crop_sd_4x3"
    elif height <= 576 and dar == "15:11":
        name = "crop_sd_15x11"

    # --- Overscan / remaining SD edge cases ---
    elif height == 608:
        name = "crop_sd_608"
    elif height == 576 and dar == "1.85:1":
        name = "crop_sd_16x9"
    elif height == 576 and aspect < 1.778:
        name = "scale_sd_4x3"
    elif width <= 768 and aspect < 1.778:
        name = "scale_sd_4x3"

    # --- Sub-HD ---
    elif height < 720 and dar == "16:9":
        name = "scale_sd_16x9"
    elif height < 720 and dar == "4:3":
        name = "sd_downscale_4x3"

    # --- HD 720p ---
    elif width == 1280 and height <= 720:
        name = "hd_16x9_letterbox"
    elif height == 720 and dar == "16:9":
        name = "hd_16x9"
    elif height == 720:
        name = "hd_16x9"

    # --- Full HD / above 720p ---
    elif width == 1920 and aspect >= 1.778:
        name = "fhd_letters"
    elif height > 720 and width <= 1920:
        name = "fhd_all"
    elif width >= 1920 and aspect < 1.778:
        name = "fhd_all"
    elif height >= 1080 and aspect >= 1.778:
        name = "fhd_letters"
    elif height > 720 and aspect >= 1.778:
        name = "fhd_letters"

    if name is None:
        return []
    return ["-vf", VIDEO_FILTERS[name]]


def get_width(fullpath: str) -> str:
    """
    Retrieve full height using mediainfo
    """
    width_raw = utils.get_metadata("Video", "Width/String", fullpath)
    clap_width_raw = utils.get_metadata("Video", "Width_CleanAperture/String", fullpath)

    width = _safe_int(width_raw, 0)
    clap_width = _safe_int(clap_width_raw, 0)

    normalised_prefix = {
        "703 ": "703",
        "720 ": "720",
        "768 ": "768",
        "1024 ": "1024",
        "1 024 ": "1024",
        "1280 ": "1280",
        "1 280 ": "1280",
        "1920 ": "1920",
        "1 920 ": "1920",
    }

    for prefix, normalised in normalised_prefix.items():
        if width.startswith("720 ") and clap_width.startswith("703 "):
            return "703"
        if width.startswith(prefix):
            return normalised

    if len(width) >= 6:
        print(f"Suspect width has multiple returned streams: {width}")
        width = _remove_stream_repeats(width, fullpath)
    if width.isdigit():
        return str(width)

    width = width.split(" pixel", maxsplit=1)[0]
    return re.sub("[^0-9]", "", width)


def get_height(fullpath: str) -> str:
    """
    Retrieve video height via mediainfo.

    Prefer sampled height when it is present and larger than the regular
    stored height, which can happen with some MXF samples.
    """

    sampled_height_raw = utils.get_metadata("Video", "Sampled_Height", fullpath)
    regular_height_raw = utils.get_metadata("Video", "Height", fullpath)

    sampled_height = _safe_int(sampled_height_raw, default=0)
    regular_height = _safe_int(regular_height_raw, default=0)

    height = str(max(sampled_height, regular_height))

    if len(height) >= 6:
        print(f"Suspect height has multiple returned streams: {height}")
        height = _remove_stream_repeats(height, fullpath)

    normalized_prefixes = {
        "480 ": "480",
        "486 ": "486",
        "576 ": "576",
        "608 ": "608",
        "720 ": "720",
        "1080 ": "1080",
        "1 080 ": "1080",
    }

    for prefix, normalized in normalized_prefixes.items():
        if height.startswith(prefix):
            return normalized

    height = height.split(" pixel", maxsplit=1)[0]
    return re.sub(r"[^0-9]", "", height)


def _safe_int(value: object, default: int = 0) -> int:
    """Convert value to int, returning default if conversion fails."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default
    

def _remove_stream_repeats(value: str, fullpath: str) -> str:
    """
    Deals with instances where height/width/DAR/PAR return
    multiple values for multiple streams - Video stream only
    """

    count = utils.get_metadata("General", "VideoCount", fullpath)
    print(f"Video stream total found: {count}")
    if not count.isnumeric():
        return value
    elif int(count) > 1:
        if len(value) % len(count) == 0:
            chop_length = len(value) // int(count)
            return value[:chop_length]
    else:
        return value