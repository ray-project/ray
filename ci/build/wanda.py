"""
Thin wrapper around the wanda binary that resolves it from Bazel runfiles.
All arguments are passed through verbatim.

Usage:
    bazel run //ci/build:wanda -- [wanda args...]

Examples:
    # Epoch-0 digest for stable cache lookups (local):
    bazel run //ci/build:wanda -- digest -epoch 0 -env_file rayci.env ci/docker/ray-core.wanda.yaml

    # Match CI build digest (in CI runner with RAYCI_* env vars):
    bazel run //ci/build:wanda -- digest -rayci
"""
from __future__ import annotations

import platform
import subprocess
import sys

import runfiles

from ci.build.build_common import BuildError, find_ray_root, log


def _wanda_binary() -> str:
    """Get the path to the wanda binary from bazel runfiles."""
    r = runfiles.Create()
    system = platform.system()
    machine = platform.machine().lower()

    if system == "Linux" and machine in ("x86_64", "amd64"):
        return r.Rlocation("wanda_linux_x86_64/file/wanda")
    elif system == "Darwin" and machine in ("arm64", "aarch64"):
        return r.Rlocation("wanda_darwin_arm64/file/wanda")
    else:
        raise BuildError(f"Unsupported platform: {system}-{machine}")


def main():
    try:
        root = find_ray_root()
        wanda = _wanda_binary()
    except BuildError as e:
        log.error(e)
        sys.exit(1)

    cmd = [wanda] + sys.argv[1:]

    log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=root)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
