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
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator

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
    def image_name(self) -> str:
        return (
            f"cr.ray.io/rayproject/ray-wheel-py{self.python_version}{self.arch_suffix}"
        )

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
            output_dir=Path(output_dir),
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

    @contextmanager
    def _container(self) -> Generator[str, None, None]:
        # Dummy command used for images without CMD/ENTRYPOINT
        res = subprocess.run(
            ["docker", "create", self.config.image_name, "true"],
            capture_output=True,
            text=True,
            check=True,
        )
        cid = res.stdout.strip()
        try:
            yield cid
        finally:
            subprocess.run(["docker", "rm", "-f", cid], capture_output=True)

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

        cmd = ["raymake", str(self.config.ray_root / RAYMAKE_SPEC)]

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

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        # TODO(andrew-anyscale): Artifact extraction will soon be built-in to
        # raymake. For now, we extract the wheels manually using docker.
        with self._container() as cid:
            log.info("Extracting wheels...")
            # List files via docker export (image has no shell for ls)
            export_proc = subprocess.Popen(
                ["docker", "export", cid], stdout=subprocess.PIPE
            )
            tar_list = subprocess.run(
                ["tar", "-tf", "-"],
                stdin=export_proc.stdout,
                capture_output=True,
                text=True,
            )
            export_proc.stdout.close()
            export_rc = export_proc.wait()
            if export_rc != 0:
                raise BuildError(f"docker export failed with exit code {export_rc}")
            if tar_list.returncode != 0:
                raise BuildError(f"tar listing failed: {tar_list.stderr}")

            # Filter for wheel files at root level (tar may output ./file or file)
            wheel_files = [
                f.lstrip("./")
                for f in tar_list.stdout.splitlines()
                if f.endswith(".whl") and "/" not in f.lstrip("./")
            ]
            if not wheel_files:
                raise BuildError("No wheel files found in image")

            extracted = []
            for f in wheel_files:
                dest = self.config.output_dir / f
                if dest.exists():
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    bak = dest.with_suffix(f".whl.backup-{ts}")
                    log.info(f"Backing up existing {dest.name} -> {bak.name}")
                    dest.replace(bak)
                subprocess.run(["docker", "cp", f"{cid}:/{f}", str(dest)], check=True)
                extracted.append(dest)
            return extracted


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
        wheels = builder.build()
        log.info(f"Success! saved to {args.output_dir}")
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
