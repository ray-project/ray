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

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from wanda_targets import (
    BuildContext,
    RayCppWheel,
    RayWheel,
    create_context,
    get_repo_root,
)


def header(msg: str) -> None:
    """Print a blue header."""
    print(f"\n\033[34;1m===> {msg}\033[0m", file=sys.stderr)


def extract_from_image(image: str, out_dir: Path, dry_run: bool = False) -> List[Path]:
    """Extract .whl files from a docker image."""
    header(f"Extracting wheels to {out_dir}...")
    out_dir.mkdir(parents=True, exist_ok=True)

    if dry_run:
        print(f"[dry-run] docker create {image}")
        return []

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
            export.wait()

            wheels = []
            for whl in Path(tmpdir).rglob("*.whl"):
                dest = out_dir / whl.name
                shutil.copy2(whl, dest)
                wheels.append(dest)
            return wheels
    finally:
        subprocess.run(["docker", "rm", container_id], capture_output=True)


def build_wheels(
    python_version: str = "3.10",
    manylinux_version: str = "",
    arch_suffix: str = "",
    hosttype: str = "",
    target: str = "ray",
    out_dir: Optional[Path] = None,
    dry_run: bool = False,
) -> None:
    """Build Ray wheels."""
    ctx = create_context(
        python_version=python_version,
        manylinux_version=manylinux_version,
        arch_suffix=arch_suffix,
        hosttype=hosttype,
        dry_run=dry_run,
    )

    if not ctx.wanda_bin or not ctx.wanda_bin.exists():
        raise FileNotFoundError(f"Wanda not found: {ctx.wanda_bin}")

    if out_dir is None:
        out_dir = get_repo_root() / ".whl"

    ray_wheel = RayWheel(ctx)
    cpp_wheel = RayCppWheel(ctx)

    # Build wheels (deps are built automatically)
    if target in ("ray", "all"):
        ray_wheel.build()
    if target in ("cpp", "all"):
        cpp_wheel.build()

    # Extract
    extract_wheels(python_version, target, out_dir, dry_run, arch_suffix)


def extract_wheels(
    python_version: str = "3.10",
    target: str = "ray",
    out_dir: Optional[Path] = None,
    dry_run: bool = False,
    arch_suffix: str = "",
) -> None:
    """Extract wheels from built images."""
    if out_dir is None:
        out_dir = get_repo_root() / ".whl"
    # Create minimal context just for image_name() calls
    ctx = BuildContext(
        env={"PYTHON_VERSION": python_version, "ARCH_SUFFIX": arch_suffix}
    )

    if target in ("ray", "all"):
        image = RayWheel(ctx).image_name()
        for w in extract_from_image(image, out_dir, dry_run):
            print(f"  {w}")
    if target in ("cpp", "all"):
        image = RayCppWheel(ctx).image_name()
        for w in extract_from_image(image, out_dir, dry_run):
            print(f"  {w}")


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
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
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(args)


def main(args: Optional[List[str]] = None) -> int:
    try:
        p = parse_args(args)
        py = p.python_version.lower().removeprefix("python").removeprefix("py")
        build_wheels(
            python_version=py,
            manylinux_version=p.manylinux_version,
            arch_suffix=p.arch_suffix,
            hosttype=p.hosttype,
            target=p.target,
            out_dir=p.out_dir,
            dry_run=p.dry_run,
        )
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        print(f"\033[31;1mError: {e}\033[0m", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
