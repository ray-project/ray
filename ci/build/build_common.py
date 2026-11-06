"""Shared utilities for local build scripts (build_wheel.py, build_image.py)."""
from __future__ import annotations

import logging
import platform as _platform
import re
import subprocess
from pathlib import Path


class BuildError(Exception):
    """Raised when a build operation fails."""


class _ColorFormatter(logging.Formatter):
    BLUE, RED, RESET = "\033[34m", "\033[31m", "\033[0m"
    COLORS = {logging.INFO: BLUE, logging.ERROR: RED}

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelno, "")
        return f"{color}[{record.levelname}]{self.RESET} {record.getMessage()}"


_handler = logging.StreamHandler()
_handler.setFormatter(_ColorFormatter())
log = logging.getLogger("raybuild")
log.propagate = False
log.addHandler(_handler)
log.setLevel(logging.INFO)


def find_ray_root() -> Path:
    """Walk up from this file and cwd looking for .rayciversion."""
    start = Path(__file__).resolve()
    for parent in [start, *start.parents]:
        if (parent / ".rayciversion").exists():
            return parent
    if (Path.cwd() / ".rayciversion").exists():
        return Path.cwd()
    raise BuildError("Could not find Ray root (missing .rayciversion).")


def parse_file(path: Path, pattern: str) -> str:
    """Extract a regex capture group from a file."""
    if not path.exists():
        raise BuildError(f"Missing {path}")
    match = re.search(pattern, path.read_text(), re.M)
    if not match:
        raise BuildError(f"Pattern {pattern} not found in {path}")
    return match.group(1).strip()


def detect_host_arch() -> str:
    """Return normalised host architecture (x86_64 or aarch64).

    Raises BuildError on unsupported OS/architecture combinations.
    """
    sys_os = _platform.system().lower()
    m = _platform.machine().lower()
    arch = (
        "x86_64"
        if m in ("amd64", "x86_64")
        else "aarch64"
        if m in ("arm64", "aarch64")
        else m
    )
    supported = {("darwin", "aarch64"), ("linux", "x86_64"), ("linux", "aarch64")}
    if (sys_os, arch) not in supported:
        raise BuildError(f"Unsupported platform: {sys_os}-{m}")
    return arch


def get_git_commit(root: Path) -> str:
    """Return the current git HEAD commit hash, or 'unknown' on failure."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"
