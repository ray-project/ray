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
import sys
from datetime import datetime, timezone as tz

import click

from ci.ray_ci.automation.crane_lib import (
    CraneError,
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


def _generate_destination_tag(commit: str, tag_suffix: str) -> str:
    """
    Generate a destination tag in the format: YYMMDD.{commit_prefix}{suffix}

    Examples:
        251215.abc1234-x86_64
        251215.abc1234-jdk-x86_64
    """
    date_str = datetime.now(tz.utc).strftime("%y%m%d")
    commit_prefix = commit[:7]

    return f"{date_str}.{commit_prefix}{tag_suffix}"


def _image_exists(tag: str) -> bool:
    """Check if a container image manifest exists using crane."""
    try:
        call_crane_manifest(tag)
        return True
    except CraneError:
        return False


def _copy_image(source: str, destination: str, dry_run: bool = False) -> None:
    """Copy a container image from source to destination using crane."""
    if dry_run:
        logger.info(f"DRY RUN: Would copy {source} -> {destination}")
        return

    logger.info(f"Copying {source} -> {destination}")
    call_crane_copy(source, destination)
    logger.info(f"Successfully copied to {destination}")


@click.command()
@click.option(
    "--rayci-work-repo",
    envvar="RAYCI_WORK_REPO",
    required=True,
    type=str,
    help="RAYCI work repository URL. Falls back to reading from RAYCI_WORK_REPO.",
)
@click.option(
    "--rayci-build-id",
    envvar="RAYCI_BUILD_ID",
    required=True,
    type=str,
    help="RAYCI build ID. Falls back to reading from RAYCI_BUILD_ID.",
)
@click.option(
    "--wanda-image-name",
    envvar="WANDA_IMAGE_NAME",
    required=True,
    type=str,
    help="Name of the Wanda-cached image (e.g., 'forge'). Falls back to reading from WANDA_IMAGE_NAME.",
)
@click.option(
    "--destination-repository",
    envvar="DESTINATION_REPOSITORY",
    required=True,
    type=str,
    help="Destination repository to copy the image to. Falls back to reading from DESTINATION_REPOSITORY.",
)
@click.option(
    "--tag-suffix",
    type=str,
    envvar="TAG_SUFFIX",
    required=True,
    help="Suffix for the tag (e.g., '-x86_64', '-jdk-x86_64'). Falls back to reading from TAG_SUFFIX.",
)
@click.option(
    "--buildkite-commit",
    envvar="BUILDKITE_COMMIT",
    required=True,
    type=str,
    help="Buildkite commit. Falls back to reading from BUILDKITE_COMMIT.",
)
@click.option(
    "--upload",
    is_flag=True,
    default=False,
    help="Upload the image to the registry. Without this flag, runs in dry-run mode.",
)
def main(
    rayci_work_repo: str,
    rayci_build_id: str,
    wanda_image_name: str,
    destination_repository: str,
    tag_suffix: str,
    buildkite_commit: str,
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

    source_tag = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"
    destination_tag = _generate_destination_tag(buildkite_commit, tag_suffix)
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
