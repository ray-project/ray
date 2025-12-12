"""
Copy Wanda-cached container images to a destination registry using crane.

Example:
    bazel run //ci/ray_ci/automation:copy_wanda_image -- \\
        --wanda-image-name manylinux-cibase \\
        --destination-registry rayproject/cibase \\
        --destination-tag abc123 \\
        --upload

Run with --help to see all options.
"""

import logging
import os
import sys
from typing import Optional

import click

from ci.ray_ci.automation.crane_lib import (
    call_crane_copy,
    call_crane_manifest,
    crane_docker_hub_login,
    crane_ecr_login,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class CopyWandaImageError(Exception):
    """Error raised when copying Wanda-cached images fails."""


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
    "--destination-registry",
    type=str,
    default=None,
    help="Destination registry to copy the image to. Defaults to DESTINATION_REGISTRY env var.",
)
@click.option(
    "--destination-tag",
    type=str,
    default=None,
    help="Tag for the image in the destination registry. Defaults to DESTINATION_TAG env var.",
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
    destination_registry: Optional[str],
    destination_tag: Optional[str],
    upload: bool,
) -> None:
    """
    Copy a Wanda-cached image to a destination registry.

    Handles authentication for both ECR (source/Wanda cache) and Docker Hub
    (destination). Requires BUILDKITE_JOB_ID for Docker Hub authentication.

    By default, runs in dry-run mode. Use --upload to actually copy images.
    """
    if not upload:
        logger.info("DRY RUN MODE - no images will be copied")

    rayci_work_repo = rayci_work_repo or os.environ.get("RAYCI_WORK_REPO")
    rayci_build_id = rayci_build_id or os.environ.get("RAYCI_BUILD_ID")
    wanda_image_name = wanda_image_name or os.environ.get("WANDA_IMAGE_NAME")
    destination_registry = destination_registry or os.environ.get(
        "DESTINATION_REGISTRY"
    )
    destination_tag = destination_tag or os.environ.get("BUILDKITE_COMMIT")

    required = {
        "RAYCI_WORK_REPO": rayci_work_repo,
        "RAYCI_BUILD_ID": rayci_build_id,
        "WANDA_IMAGE_NAME": wanda_image_name,
        "DESTINATION_REGISTRY": destination_registry,
        "BUILDKITE_COMMIT": destination_tag,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise CopyWandaImageError(f"Missing required values: {', '.join(missing)}")

    source_tag = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"
    destination_tag = f"{destination_registry}:{destination_tag}"

    logger.info(f"Source tag (Wanda): {source_tag}")
    logger.info(f"Target tag: {destination_tag}")

    # Authenticate crane with ECR (source registry) and Docker Hub (destination)
    ecr_registry = rayci_work_repo.split("/")[0]
    crane_ecr_login(ecr_registry)
    crane_docker_hub_login()

    # Check if target already exists (only in upload mode)
    if upload:
        logger.info("Checking if image already exists at target...")
        if _image_exists(destination_tag):
            logger.info(f"Image already exists: {destination_tag}")
            logger.info("Nothing to do, exiting successfully")
            return
        logger.info("Image does not exist, proceeding with copy")

    logger.info("Verifying source image in Wanda cache...")
    if not _image_exists(source_tag):
        raise CopyWandaImageError(
            f"Source image not found in Wanda cache: {source_tag}"
        )

    # Copy image
    _copy_image(source_tag, destination_tag, dry_run=not upload)


if __name__ == "__main__":
    main()
