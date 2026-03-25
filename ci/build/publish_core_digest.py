"""
Publish a ray-core build image to S3 keyed by its wanda digest.

Exports the Docker image from the wanda cache registry and uploads it
to S3 so local developers can download it by computing the same digest.

Required env vars:
    PYTHON_VERSION, ARCH_SUFFIX, HOSTTYPE  — wanda build args
    RAYCI_WORK_REPO, RAYCI_BUILD_ID        — wanda cache coordinates
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile

from ci.build.build_common import BuildError, find_ray_root, log

WANDA_SPEC = "ci/docker/ray-core.wanda.yaml"
ENV_FILE = "rayci.env"


def s3_key(platform: str, arch: str, python_version: str, digest: str) -> str:
    """Construct the S3 key for a core digest image."""
    return f"core-digest/{platform}/{arch}/{python_version}/{digest}"


def image_tag(work_repo: str, build_id: str, python_version: str) -> str:
    """Construct the wanda work tag for the ray-core image."""
    return f"{work_repo}:{build_id}-ray-core-py{python_version}"


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise BuildError(f"Required environment variable {name} is not set")
    return value


def compute_digest(ray_root: str) -> str:
    """Compute the epoch-0 wanda digest for the ray-core spec."""
    cmd = [
        "bazel",
        "run",
        "//ci/build:wanda",
        "--",
        "digest",
        "-epoch",
        "0",
        "-env_file",
        ENV_FILE,
        WANDA_SPEC,
    ]
    log.info(f"Computing digest: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=ray_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise BuildError(
            f"wanda digest failed (rc={result.returncode}): {result.stderr.strip()}"
        )
    digest = result.stdout.strip()
    if not digest:
        raise BuildError("wanda digest produced empty output")
    return digest


def export_image(tag: str, output_path: str) -> None:
    """Export a Docker image to a tar file using crane."""
    cmd = ["crane", "export", tag, output_path]
    log.info(f"Exporting image: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise BuildError(f"crane export failed (rc={result.returncode})")


def upload_to_s3(ray_root: str, key: str, path: str) -> None:
    """Upload a file to S3 via copy_files.py."""
    cmd = [
        "bazel",
        "run",
        ".buildkite:copy_files",
        "--",
        "--destination",
        "ray-core",
        "--key",
        key,
        "--path",
        path,
    ]
    log.info(f"Uploading: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=ray_root)
    if result.returncode != 0:
        raise BuildError(f"upload failed (rc={result.returncode})")


def publish(dry_run: bool = False) -> None:
    """Compute digest, export image, and upload to S3."""
    root = str(find_ray_root())

    python_version = _require_env("PYTHON_VERSION")
    work_repo = _require_env("RAYCI_WORK_REPO")
    build_id = _require_env("RAYCI_BUILD_ID")

    digest = compute_digest(root)
    tag = image_tag(work_repo, build_id, python_version)
    key = s3_key("linux", "x86_64", python_version, digest)

    log.info(f"Digest: {digest}")
    log.info(f"Image tag: {tag}")
    log.info(f"S3 key: {key}")

    if dry_run:
        log.info("Dry run — skipping export and upload")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        tar_path = os.path.join(tmpdir, f"ray-core-py{python_version}.tar")
        export_image(tag, tar_path)
        upload_to_s3(root, key, tar_path)

    log.info("Published successfully")


def main():
    parser = argparse.ArgumentParser(
        description="Publish ray-core image to S3 keyed by wanda digest.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without exporting or uploading.",
    )
    args = parser.parse_args()

    try:
        publish(dry_run=args.dry_run)
    except BuildError as e:
        log.error(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
