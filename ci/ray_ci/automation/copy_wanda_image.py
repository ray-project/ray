"""
Copy Wanda-cached container images to a destination registry using crane.

Example:
    bazel run //ci/ray_ci/automation:copy_wanda_image -- \\
        --wanda-image-name manylinux-cibase \\
        --destination-repository rayproject/manylinux2014 \\
        --tag-suffix -x86_64 \\
        --upload

Tags are generated in the format: YYMMDD.{commit_prefix}.{suffix}
For example: 251215.abc1234-x86_64

Run with --help to see all options.
"""

import logging
import os
import sys
from datetime import datetime, timezone as tz
from typing import Optional

import click

from ci.ray_ci.automation.crane_lib import (
    call_crane_copy,
    call_crane_manifest,
)
from ci.ray_ci.utils import ecr_docker_login

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class CopyWandaImageError(Exception):
    """Error raised when copying Wanda-cached images fails."""


def _generate_destination_tag(commit: str, tag_suffix: Optional[str] = None) -> str:
    """
    Generate a destination tag in the format: YYMMDD.{commit_prefix}{suffix}

    Examples:
        251215.abc1234-x86_64
        251215.abc1234-jdk-x86_64
    """
    date_str = datetime.now(tz.utc).strftime("%y%m%d")
    commit_prefix = commit[:7]
    if tag_suffix:
        return f"{date_str}.{commit_prefix}{tag_suffix}"
    return f"{date_str}.{commit_prefix}"


def _image_exists(tag: str) -> bool:
    """Check if a container image manifest exists using crane."""
    return_code, _ = call_crane_manifest(tag)
    return return_code == 0


def _copy_image(source: str, destination: str, dry_run: bool = False) -> None:
    """Copy a container image from source to destination using crane."""
    if dry_run:
        logger.info(f"DRY RUN: Would copy {source} -> {destination}")
        return

    logger.info(f"Copying {source} -> {destination}")
    return_code, output = call_crane_copy(source, destination)
    if return_code != 0:
        raise CopyWandaImageError(f"Crane copy failed: {output}")
    logger.info(f"Successfully copied to {destination}")


@click.command()
@click.option(
    "--rayci-work-repo",
    type=str,
    default=None,
    help="RAYCI work repository URL. Defaults to RAYCI_WORK_REPO env var.",
)
@click.option(
    "--rayci-build-id",
    type=str,
    default=None,
    help="RAYCI build ID. Defaults to RAYCI_BUILD_ID env var.",
)
@click.option(
    "--wanda-image-name",
    type=str,
    default=None,
    help="Name of the Wanda-cached image (e.g., 'forge'). Defaults to WANDA_IMAGE_NAME env var.",
)
@click.option(
    "--destination-repository",
    type=str,
    default=None,
    help="Destination repository to copy the image to. Defaults to DESTINATION_REPOSITORY env var.",
)
@click.option(
    "--tag-suffix",
    type=str,
    default=None,
    help="Suffix for the tag (e.g., '-x86_64', '-jdk-x86_64'). Defaults to TAG_SUFFIX env var.",
)
@click.option(
    "--upload",
    is_flag=True,
    default=False,
    help="Upload the image to the registry. Without this flag, runs in dry-run mode.",
)
def main(
    rayci_work_repo: Optional[str],
    rayci_build_id: Optional[str],
    wanda_image_name: Optional[str],
    destination_repository: Optional[str],
    tag_suffix: Optional[str],
    upload: bool,
) -> None:
    """
    Copy a Wanda-cached image to a destination registry.

    Handles authentication for both ECR (source/Wanda cache) and Docker Hub
    (destination). Requires BUILDKITE_JOB_ID for Docker Hub authentication.

    Tags are generated in the format: YYMMDD.{commit_prefix}{suffix}
    For example: 251215.abc1234-x86_64

    By default, runs in dry-run mode. Use --upload to actually copy images.
    """
    if not upload:
        logger.info("DRY RUN MODE - no images will be copied")

    rayci_work_repo = rayci_work_repo or os.environ.get("RAYCI_WORK_REPO")
    rayci_build_id = rayci_build_id or os.environ.get("RAYCI_BUILD_ID")
    wanda_image_name = wanda_image_name or os.environ.get("WANDA_IMAGE_NAME")
    destination_repository = destination_repository or os.environ.get(
        "DESTINATION_REPOSITORY"
    )
    tag_suffix = tag_suffix or os.environ.get("TAG_SUFFIX")
    commit = os.environ.get("BUILDKITE_COMMIT")

    required = {
        "RAYCI_WORK_REPO": rayci_work_repo,
        "RAYCI_BUILD_ID": rayci_build_id,
        "WANDA_IMAGE_NAME": wanda_image_name,
        "DESTINATION_REPOSITORY": destination_repository,
        "BUILDKITE_COMMIT": commit,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise CopyWandaImageError(f"Missing required values: {', '.join(missing)}")

    source_tag = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"
    destination_tag = _generate_destination_tag(commit, tag_suffix)
    full_destination = f"{destination_repository}:{destination_tag}"

    logger.info(f"Source tag (Wanda): {source_tag}")
    logger.info(f"Target tag: {full_destination}")

    # Authenticate with ECR (source registry). Docker Hub authentication is
    # handled by copy_files.py.
    ecr_registry = rayci_work_repo.split("/")[0]
    ecr_docker_login(ecr_registry)

    logger.info("Verifying source image in Wanda cache...")
    if not _image_exists(source_tag):
        raise CopyWandaImageError(
            f"Source image not found in Wanda cache: {source_tag}"
        )

    # Copy image
    _copy_image(source_tag, full_destination, dry_run=not upload)


if __name__ == "__main__":
    main()
