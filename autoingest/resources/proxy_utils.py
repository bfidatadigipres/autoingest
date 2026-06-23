import os
import re
import pytz
import subprocess
from datetime import datetime, timezone
from typing import Optional, Union
import autoingest.resources.utils as utils

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


def build_audio_args(
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


def select_video_filter(
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


def check_mod_time(fpath: str) -> bool:
    """
    See if mod time over 5 hrs old
    """
    now = datetime.now().astimezone()
    local_tz = pytz.timezone("Europe/London")
    file_mod_time = os.stat(fpath).st_mtime
    modified = datetime.fromtimestamp(file_mod_time, tz=timezone.utc)
    mod = modified.replace(tzinfo=pytz.utc).astimezone(local_tz)

    diff = now - mod
    seconds = diff.seconds
    hours = (seconds / 60) // 60

    print(f"{fpath}\tModified time is {seconds} seconds ago")
    if seconds < 18000:
        return True
    return False


def get_width(fullpath: str) -> int:
    """
    Retrieve full height using mediainfo
    """
    width_raw = utils.get_metadata("Video", "Width/String", fullpath)
    clap_width_raw = utils.get_metadata("Video", "Width_CleanAperture/String", fullpath)

    width = width_raw.strip() if width_raw else ""
    clap_width = clap_width_raw.strip() if clap_width_raw else ""

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
            return 703
        if width.startswith(prefix):
            return int(normalised)

    if len(width) >= 6:
        print(f"Suspect width has multiple returned streams: {width}")
        width = _remove_stream_repeats(width, fullpath)
    if width.isdigit():
        return int(width)

    width = width.split(" pixel", maxsplit=1)[0]
    digits_only = re.sub(r"[^0-9]", "", width)
    if not digits_only:
        return 0
    return int(digits_only)


def get_height(fullpath: str) -> int:
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
            return int(normalized)

    height = height.split(" pixel", maxsplit=1)[0]
    digits_only = re.sub(r"[^0-9]", "", height)
    if not digits_only:
        return 0
    return int(digits_only)


def get_dar(fullpath: str) -> str:
    """
    Retrieves metadata DAR info and returns as string
    """

    dar_setting = utils.get_metadata("Video", "DisplayAspectRatio/String", fullpath)
    if len(dar_setting) >= 6:
        print(f"Suspect height has multiple returned streams: {dar_setting}")
        dar_setting = _remove_stream_repeats(dar_setting, fullpath)

    settings = {
        "4:3": "4:3",
        "16:9": "16:9",
        "15:11": "4:3",
        "1.85:1": "1.85:1",
        "2.2:1": "2.2:1",
    }
    
    for key, value in settings.items():
        if key in str(dar_setting):
            return value

    return str(dar_setting)


def get_par(fullpath: str) -> str:
    """
    Retrieves metadata PAR info and returns
    Checks if multiples from multi video tracks
    """

    par_setting = utils.get_metadata("Video", "PixelAspectRatio", fullpath)
    par_full = str(par_setting).rstrip("\n")
    if len(par_full) >= 6:
        print(f"Suspect height has multiple returned streams: {par_full}")
        par_full = _remove_stream_repeats(par_full, fullpath)

    if len(par_full) <= 5:
        return par_full
    return par_full[:5]


def _safe_int(value: object, default: int = 0) -> int:
    """Convert value to int, returning default if conversion fails."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _remove_stream_repeats(value: str, fullpath: str) -> str | None:
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
    return None


def check_audio(
    fullpath: str,
) -> tuple[Optional[str], Optional[str], Optional[Union[bytes, list[str]]]]:
    """
    FFprobe command to retrieve channels, identify
    stereo or mono, returned as 2 or 1 respectively
    """

    cmd0 = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=index:stream_tags=language",
        "-of",
        "compact=p=0:nk=1",
        fullpath,
    ]

    cmd1 = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:1",
        "-show_entries",
        "stream=index:stream_tags=language",
        "-of",
        "compact=p=0:nk=1",
        fullpath,
    ]

    cmd2 = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=index",
        "-of",
        "compact=p=0",
        fullpath,
    ]

    audio = utils.get_metadata("Audio", "Format", fullpath)
    if len(audio) == 0:
        return None, None, None

    try:
        lang0 = subprocess.check_output(cmd0)
        lang0_str = lang0.decode("utf-8")
    except (subprocess.CalledProcessError, Exception):
        lang0_str = ""
    try:
        lang1 = subprocess.check_output(cmd1)
        lang1_str = lang1.decode("utf-8")
    except (subprocess.CalledProcessError, Exception):
        lang1_str = ""
    try:
        streams = subprocess.check_output(cmd2)
        streams_str = streams.decode("utf-8").lstrip("\n").rstrip("\n").split("\n")
    except (subprocess.CalledProcessError, Exception):
        streams_str = None
    print(f"**** LANGUAGES: Stream 0 {lang0_str} - Stream 1 {lang1_str}")

    if "nar" in str(lang0_str).lower():
        print("Narration stream 0 / English stream 1")
        return ("Audio", "1", streams_str)
    elif "nar" in str(lang1_str).lower():
        print("Narration stream 1 / English stream 0")
        return ("Audio", "0", streams_str)
    else:
        return ("Audio", None, streams_str)


def get_duration(fullpath: str) -> Optional[tuple[int, str]]:
    """
    Retrieves duration information via mediainfo
    where more than two returned, file longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    """

    duration = utils.get_metadata("Video", "Duration", fullpath)
    if not duration:
        return (0, "")
    if "." in duration:
        duration = duration.split(".")

    if isinstance(duration, str):
        second_duration = int(duration) // 1000
        return (second_duration, "0")
    elif len(duration) == 2:
        print("Just one duration returned")
        num = duration[0]
        second_duration = int(num) // 1000
        print(second_duration)
        return (second_duration, "0")
    elif len(duration) > 2:
        print("More than one duration returned")
        dur1 = f"{duration[0]}"
        dur2 = f"{duration[1][6:]}"
        print(dur1, dur2)
        if int(dur1) > int(dur2):
            second_duration = int(dur1) // 1000
            return (second_duration, "0")
        elif int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return (second_duration, "1")


def check_for_mixed_audio(fpath: str) -> Optional[dict[str, int]]:
    """
    For use where audio channels 6+ exist
    check for 'DL' and 'DR' and build different
    FFmpeg command that uses mixed audio only
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=channel_layout",
        "-of",
        "csv=p=0",
        fpath,
    ]
    audio = subprocess.check_output(cmd)
    audio_str = str(audio.decode("utf-8").lstrip("\n").rstrip("\n"))
    audio_channels = str(audio_str).split("\n")
    if len(audio_channels) > 1:
        audio_downmix = {}
        for num in range(0, len(audio_channels)):
            if "(DL)" in audio_channels[num]:
                audio_downmix["DL"] = num
            if "(DR)" in audio_channels[num]:
                audio_downmix["DR"] = num
        if len(audio_downmix) == 2:
            return audio_downmix

    return None


def check_for_fl_fr(fpath: str) -> bool:
    """
    For use where audio is '1 channels (FL) or (FR)
    which is unsupported by FFmpeg, add -ac 2 to command
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=channel_layout",
        "-of",
        "csv=p=0",
        fpath,
    ]
    audio = subprocess.check_output(cmd)
    audio_str = str(audio.decode("utf-8")).lstrip("\n").rstrip("\n")
    audio_channels = audio_str.split("\n")
    if "5.1(side)" in audio_channels:
        return True
    if len(audio_channels) > 1:
        audio_downmix = {}
        for num in range(0, len(audio_channels)):
            if "1 channels (FL)" in audio_channels[num]:
                audio_downmix["FL"] = num
            if "1 channels (FR)" in audio_channels[num]:
                audio_downmix["FR"] = num
        if len(audio_downmix) == 2:
            return True
    else:
        if "5.1" in audio_channels:
            return True
    return False


def check_for_twelve_channel_audio(fullpath: str) -> bool:
    """
    Additional check for increase in complex audio
    """
    twelve_chnl = False
    discretes = utils.get_metadata("Audio", "ChannelLayout", fullpath)
    if "Discrete" in discretes:
        if discretes.count("Discrete") >= 12:
            twelve_chnl = True
    audio_channels = utils.get_metadata("General", "Audio_Channels_Total", fullpath)
    audio_count = utils.get_metadata("General", "AudioCount", fullpath)
    if audio_count.strip() == "1" and audio_channels.strip() == "12":
        twelve_chnl = True

    return twelve_chnl


def call_ffmpeg_command(ffmpeg_cmd: list[str]) -> subprocess.CompletedProcess:
    """
    Call up subprocess
    with FFmpeg command
    """

    try:
        result = subprocess.run(
            ffmpeg_cmd,
            shell=False,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        print(e)
        result = subprocess.CompletedProcess(args=ffmpeg_cmd, returncode=-1, stderr=str(e), stdout="")
    
    return result


def adjust_seconds(duration: float, data: str) -> float:
    """
    Adjust second durations within
    FFmpeg detected blackspace
    """
    blist = _retrieve_blackspaces(data)
    print(f"*** BLACK GAPS: {blist}")
    if not blist:
        return duration // 2

    secs = duration // 4
    clash = _check_seconds(blist, secs)
    if not clash:
        return secs

    for num in range(2, 5):
        frame_secs = duration // num
        clash = _check_seconds(blist, frame_secs)
        if not clash:
            return frame_secs

    if len(blist) > 2:
        first = blist[1].split(" - ")[1]
        second = blist[2].split(" - ")[0]
        frame_secs = int(first) + (int(second) - int(first)) // 2
        if int(first) < frame_secs < int(second):
            return frame_secs

    return duration // 2


def _retrieve_blackspaces(data: str) -> list[str]:
    """
    Retrieve black detect log and check if
    second variable falls in blocks of blackdetected
    """
    data_list = data.splitlines()
    time_range = []
    for line in data_list:
        if "black_start" in line:
            split_line = line.split(":")
            split_start = split_line[1].split(".")[0]
            start = re.sub("[^0-9]", "", split_start)
            split_end = split_line[2].split(".")[0]
            end = re.sub("[^0-9]", "", split_end)
            # Round up to next second for cover
            end = str(int(end) + 1)
            time_range.append(f"{start} - {end}")
    return time_range


def _check_seconds(blackspace: list[str], seconds: float) -> Optional[bool]:
    """
    Create range and check for second within
    """
    clash = []
    for item in blackspace:
        start, end = item.split(" - ")
        st = int(start) - 1
        ed = int(end) + 1
        if seconds in range(st, ed):
            clash.append(seconds)

    if len(clash) > 0:
        return True


def get_jpeg(seconds: float, fullpath: str, outpath: str) -> bool:
    """
    Retrieve JPEG from MP4
    Seconds accepted as float
    """
    cmd = [
        "ffmpeg",
        "-ss",
        str(seconds),
        "-i",
        fullpath,
        "-frames:v",
        "1",
        "-q:v",
        "2",
        outpath,
    ]

    command = " ".join(cmd)
    print(command)
    try:
        subprocess.call(cmd)
        if os.path.isfile(outpath):
            return True
    except subprocess.CalledProcessError as err:
        print(f"get_jpeg(): failed to extract JPEG: {command} {err}")
    return False


def make_jpg(
    filepath: str, arg: str, transcode_pth: Optional[str], percent: Optional[str]
) -> Optional[str]:
    """
    Create GM JPEG using command based on argument
    These command work. For full size don't use resize.
    """

    start_reduce = ["gm", "convert", "-density", "300x300", filepath, "-strip"]
    start = ["gm", "convert", "-density", "600x600", filepath, "-strip"]
    thumb = ["-resize", "x180"]
    oversize = ["-resize", f"{percent}%x{percent}%"]

    if not transcode_pth:
        out = os.path.splitext(filepath)[0]
    else:
        fname = os.path.split(filepath)[1]
        file = os.path.splitext(fname)[0]
        out = os.path.join(transcode_pth, file)

    if "thumb" in arg:
        outfile = f"{out}_thumbnail.jpg"
        cmd = start_reduce + thumb + [f"{outfile}"]
    elif "oversize" in arg:
        outfile = f"{out}_largeimage.jpg"
        cmd = start + oversize + [f"{outfile}"]
    else:
        outfile = f"{out}_largeimage.jpg"
        cmd = start + [f"{outfile}"]

    try:
        subprocess.call(cmd)
    except subprocess.CalledProcessError as err:
        print(f"ERROR: JPEG creation failed for filepath: {filepath} {err}")
    os.chmod(outfile, 0o777)
    if os.path.exists(outfile):
        return outfile
