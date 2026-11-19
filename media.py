"""Use FFmpeg to find hard cuts and extract representative frames."""

import json
import re
import subprocess
from decimal import Decimal
from pathlib import Path

PTS_TIME = re.compile(r"\bpts_time:([0-9.eE+-]+)")
SCENE_THRESHOLD = 0.30


def run(*args: str) -> str:
    return subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    ).stdout


def microseconds(value: str) -> int:
    return round(Decimal(value) * 1_000_000)


def shot_ranges(source: Path) -> list[tuple[int, int]]:
    probe = json.loads(
        run(
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=duration:format=duration",
            "-of", "json", str(source),
        )
    )
    streams = probe.get("streams")
    if not streams:
        raise ValueError("Input contains no video stream")
    duration = streams[0].get("duration")
    if duration in (None, "N/A"):
        duration = probe.get("format", {}).get("duration")
    if duration in (None, "N/A") or (end := microseconds(duration)) <= 0:
        raise ValueError("Input needs a known, positive video duration")

    scene_filter = (
        "setpts=PTS-STARTPTS,"
        f"select=gt(scene\\,{SCENE_THRESHOLD}),"
        "metadata=print:key=lavfi.scene_score:file=-"
    )
    output = run(
        "ffmpeg", "-nostdin", "-v", "error", "-i", str(source),
        "-map", "0:v:0", "-vf", scene_filter, "-an", "-f", "null", "-",
    )
    cuts = {microseconds(value) for value in PTS_TIME.findall(output)}
    boundaries = [0, *sorted(cut for cut in cuts if 0 < cut < end), end]
    return list(zip(boundaries, boundaries[1:]))


def extract_frame(source: Path, start: int, end: int, output: Path) -> None:
    midpoint = Decimal(start + end) / 2_000_000
    run(
        "ffmpeg", "-nostdin", "-v", "error", "-y", "-ss", str(midpoint),
        "-i", str(source), "-map", "0:v:0", "-frames:v", "1",
        "-q:v", "3", str(output),
    )
    if not output.is_file():
        # A short final shot can end between frame timestamps.
        run(
            "ffmpeg", "-nostdin", "-v", "error", "-y",
            "-ss", str(Decimal(start) / 1_000_000), "-i", str(source),
            "-map", "0:v:0", "-frames:v", "1", "-q:v", "3", str(output),
        )
