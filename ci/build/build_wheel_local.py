#!/usr/bin/env python3
"""
Build Ray wheel files locally.
Usage:
    python build_wheel_local.py [PYTHON_VERSION] [TARGET]
Examples:
    python build_wheel_local.py 3.11
    python build_wheel_local.py 3.12 cpp
    python build_wheel_local.py 3.11 all
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from wanda_targets import (
    RayCppWheel,
    RayWheel,
    create_context,
)


def header(msg: str) -> None:
    """Print a blue header."""
    print(f"\n\033[34;1m===> {msg}\033[0m", file=sys.stderr)


def extract_from_image(image: str, out_dir: Path) -> list[Path]:
    """Extract .whl files from a docker image."""
    header(f"Extracting wheels to {out_dir}...")
    out_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        ["docker", "create", image, "true"],
        capture_output=True,
        text=True,
        check=True,
    )
    container_id = result.stdout.strip()

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            export = subprocess.Popen(
                ["docker", "export", container_id], stdout=subprocess.PIPE
            )
            subprocess.run(["tar", "-x", "-C", tmpdir], stdin=export.stdout, check=True)
            if export.wait() != 0:
                raise subprocess.CalledProcessError(export.returncode, export.args)

            wheels = []
            for whl in Path(tmpdir).rglob("*.whl"):
                dest = out_dir / whl.name
                shutil.copy2(whl, dest)
                wheels.append(dest)
            return wheels
    finally:
        result = subprocess.run(["docker", "rm", container_id], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Warning: failed to remove container {container_id}: {result.stderr}", file=sys.stderr)


def build_wheels(
    python_version: str = "3.10",
    manylinux_version: str = "",
    arch_suffix: str = "",
    hosttype: str = "",
    target: str = "ray",
) -> tuple[RayWheel, RayCppWheel]:
    """Build Ray wheels and return the wheel targets."""
    ctx = create_context(
        python_version=python_version,
        manylinux_version=manylinux_version,
        arch_suffix=arch_suffix,
        hosttype=hosttype,
    )

    ray_wheel = RayWheel(ctx)
    cpp_wheel = RayCppWheel(ctx)

    if target in ("ray", "all"):
        ray_wheel.build()
    if target in ("cpp", "all"):
        cpp_wheel.build()

    return ray_wheel, cpp_wheel


def extract_wheels(
    ray_wheel: RayWheel,
    cpp_wheel: RayCppWheel,
    target: str = "ray",
    out_dir: Path | None = None,
) -> list[Path]:
    """Extract wheels from built images."""
    if out_dir is None:
        out_dir = ray_wheel.ctx.repo_root / ".whl"

    wheels: list[Path] = []
    if target in ("ray", "all"):
        extracted = extract_from_image(ray_wheel.remote_image, out_dir)
        for w in extracted:
            print(f"  {w}")
        wheels.extend(extracted)
    if target in ("cpp", "all"):
        extracted = extract_from_image(cpp_wheel.remote_image, out_dir)
        for w in extracted:
            print(f"  {w}")
        wheels.extend(extracted)
    return wheels


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Ray wheels locally",
        usage="%(prog)s [python_version] [target] [options]\n"
        "       %(prog)s 3.11              # build ray wheel for py3.11\n"
        "       %(prog)s 3.12 cpp          # build cpp wheel for py3.12",
    )
    parser.add_argument("python_version", nargs="?", default="3.10")
    parser.add_argument(
        "target", nargs="?", default="ray", choices=["ray", "cpp", "all"]
    )
    parser.add_argument("--manylinux-version", default="")
    parser.add_argument("--arch-suffix", default="")
    parser.add_argument("--hosttype", default="")
    parser.add_argument("--out-dir", type=Path)
    return parser.parse_args(args)


def main(args: list[str] | None = None) -> int:
    try:
        p = parse_args(args)
        py = p.python_version.lower().removeprefix("python").removeprefix("py")

        ray_wheel, cpp_wheel = build_wheels(
            python_version=py,
            manylinux_version=p.manylinux_version,
            arch_suffix=p.arch_suffix,
            hosttype=p.hosttype,
            target=p.target,
        )

        extract_wheels(ray_wheel, cpp_wheel, p.target, p.out_dir)

        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        print(f"\033[31;1mError: {e}\033[0m", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
