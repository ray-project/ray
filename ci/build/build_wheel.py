#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
"""
Build Ray manylinux wheels locally using raymake.
"""
from __future__ import annotations

import argparse
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# Configuration Constants
SUPPORTED_PYTHON_VERSIONS = ("3.10", "3.11", "3.12", "3.13")
RAYMAKE_SPEC = "ci/docker/ray-wheel.wanda.yaml"


class _ColorFormatter(logging.Formatter):
    BLUE, RED, RESET = "\033[34m", "\033[31m", "\033[0m"
    COLORS = {logging.INFO: BLUE, logging.ERROR: RED}

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelno, "")
        return f"{color}[{record.levelname}]{self.RESET} {record.getMessage()}"


_handler = logging.StreamHandler()
_handler.setFormatter(_ColorFormatter())
log = logging.getLogger("raybuild")
log.addHandler(_handler)
log.setLevel(logging.INFO)


class BuildError(Exception):
    """Raised when a build operation fails."""


@dataclass(frozen=True)
class BuildConfig:
    python_version: str
    output_dir: Path
    ray_root: Path
    hosttype: str
    arch_suffix: str
    raymake_version: str
    manylinux_version: str
    commit: str

    @property
    def build_env(self) -> dict[str, str]:
        return {
            "PYTHON_VERSION": self.python_version,
            "MANYLINUX_VERSION": self.manylinux_version,
            "HOSTTYPE": self.hosttype,
            "ARCH_SUFFIX": self.arch_suffix,
            "BUILDKITE_COMMIT": self.commit,
            "IS_LOCAL_BUILD": "true",
        }

    @classmethod
    def from_env(cls, python_version: str, output_dir: str) -> BuildConfig:
        root = cls._find_ray_root()
        host, suffix = cls._detect_platform()

        # Extract metadata
        rayciversion_path = root / ".rayciversion"
        if not rayciversion_path.exists():
            raise BuildError(f"Missing {rayciversion_path}")
        raymake_version = rayciversion_path.read_text().strip()
        manylinux_version = cls._parse_file(
            root / "rayci.env", r'MANYLINUX_VERSION=["\']?([^"\'\s]+)'
        )
        commit = cls._get_git_commit(root)

        return cls(
            python_version=python_version,
            output_dir=Path(output_dir).resolve(),
            ray_root=root,
            hosttype=host,
            arch_suffix=suffix,
            raymake_version=raymake_version,
            manylinux_version=manylinux_version,
            commit=commit,
        )

    @staticmethod
    def _find_ray_root() -> Path:
        start = Path(__file__).resolve()
        for parent in [start, *start.parents]:
            if (parent / ".rayciversion").exists():
                return parent
        if (Path.cwd() / ".rayciversion").exists():
            return Path.cwd()
        raise BuildError("Could not find Ray root (missing .rayciversion).")

    @staticmethod
    def _detect_platform() -> tuple[str, str]:
        """Return (hosttype, arch_suffix) for current platform."""
        sys_os = platform.system()
        m = platform.machine().lower()
        arch = (
            "x86_64"
            if m in ("amd64", "x86_64")
            else "aarch64"
            if m in ("arm64", "aarch64")
            else m
        )

        mapping = {
            ("Darwin", "aarch64"): ("aarch64", "-aarch64"),
            ("Linux", "x86_64"): ("x86_64", ""),
            ("Linux", "aarch64"): ("aarch64", "-aarch64"),
        }
        if (sys_os, arch) not in mapping:
            raise BuildError(f"Unsupported platform: {sys_os}-{m}")
        return mapping[(sys_os, arch)]

    @staticmethod
    def _parse_file(path: Path, pattern: str) -> str:
        if not path.exists():
            raise BuildError(f"Missing {path}")
        match = re.search(pattern, path.read_text(), re.M)
        if not match:
            raise BuildError(f"Pattern {pattern} not found in {path}")
        return match.group(1).strip()

    @staticmethod
    def _get_git_commit(root: Path) -> str:
        try:
            return subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=root,
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except Exception:
            return "unknown"


class WheelBuilder:
    def __init__(self, config: BuildConfig):
        self.config = config

    def build(self) -> list[Path]:
        if not shutil.which("raymake"):
            raise BuildError("raymake not found. Run via ./build-wheel.sh")

        log.info("Build configuration:")
        summary = {
            "Python": self.config.python_version,
            "Arch": self.config.hosttype,
            "Commit": self.config.commit,
            "Raymake": self.config.raymake_version,
            "Manylinux": self.config.manylinux_version,
            "Ray Root": self.config.ray_root,
            "Output Dir": self.config.output_dir,
        }
        print("-" * 50)
        for k, v in summary.items():
            print(f"{k:<12}: {v}")
        print("-" * 50)

        cmd = [
            "raymake",
            "--artifacts_dir",
            str(self.config.output_dir),
            str(self.config.ray_root / RAYMAKE_SPEC),
        ]

        log.info(f"Running raymake: {RAYMAKE_SPEC}")
        log.info(f"Build environment: {self.config.build_env}")
        proc = subprocess.Popen(cmd, env={**os.environ, **self.config.build_env})
        try:
            if proc.wait() != 0:
                raise BuildError("raymake failed")
        except KeyboardInterrupt:
            # We don't need to do much here; the child received the SIGINT too.
            # Just wait a moment for it to exit to avoid the "No such process" error.
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
            raise

        return list(self.config.output_dir.glob("*.whl"))


def main():
    # Override the program name to match the root script name
    parser = argparse.ArgumentParser(
        prog="./build-wheel.sh",
        description="Build Ray wheel using a manylinux base image.",
        epilog="Example: ./build-wheel.sh 3.13",
    )

    parser.add_argument(
        "python_version",
        choices=SUPPORTED_PYTHON_VERSIONS,
        metavar="PYTHON_VERSION",
        help="Target Python version: %(choices)s",
    )

    parser.add_argument(
        "output_dir",
        nargs="?",
        default=".whl",
        help="Directory to store wheels (default: .whl)",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    try:
        builder = WheelBuilder(
            BuildConfig.from_env(args.python_version, args.output_dir)
        )
        output_dir = builder.config.output_dir
        if output_dir.exists():
            if not output_dir.is_dir():
                raise BuildError(
                    f"Output path '{output_dir}' exists and is not a directory."
                )
            existing_wheels = list(output_dir.glob("*.whl"))
            if existing_wheels:
                ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                backup_dir = output_dir / f"old_{ts}"
                backup_dir.mkdir()
                for whl in existing_wheels:
                    whl.rename(backup_dir / whl.name)
                log.info(f"Moved existing wheels to {backup_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        wheels = builder.build()
        log.info(f"Success! Wheels saved to {output_dir}")
        log.info("To install, run:")
        for w in wheels:
            print(f"  pip install {w}")
    except BuildError as e:
        log.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
