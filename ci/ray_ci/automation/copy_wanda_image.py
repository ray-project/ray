"""
CLI tool for copying Wanda-cached container images to a target registry.

This script copies images that have been built and cached by Wanda to another
container registry. It uses crane to perform the copy operation.

Usage:
    bazel run //ci/ray_ci/automation:copy_wanda_image -- [--upload]

Environment Variables:
    RAYCI_WORK_REPO: The Wanda work repository URL (source registry)
    RAYCI_BUILD_ID: Build ID for the Wanda build
    WANDA_IMAGE_NAME: Name of the Wanda-cached image (e.g., "forge", "manylinux-cibase")
    TARGET_REPO: Target repository (e.g., "rayproject/my-image")
    BUILDKITE_COMMIT: The commit hash for tagging the destination image
"""

import logging
import os
import sys
from typing import Optional

import click

from ci.ray_ci.automation.crane_lib import call_crane_copy, call_crane_manifest


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
    "--upload",
    is_flag=True,
    default=False,
    help="Upload the image to the registry. Without this flag, runs in dry-run mode.",
)
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
    "--target-registry",
    type=str,
    default=None,
    help="Target registry. Defaults to TARGET_REGISTRY env var.",
)
@click.option(
    "--commit-hash",
    type=str,
    default=None,
    help="Commit hash for tagging the image. Defaults to BUILDKITE_COMMIT env var.",
)
def main(
    upload: bool,
    rayci_work_repo: Optional[str],
    rayci_build_id: Optional[str],
    wanda_image_name: Optional[str],
    target_registry: Optional[str],
    commit_hash: Optional[str],
) -> None:
    """
    Copy a Wanda-cached image to a target registry.

    Assumes that the Wanda image is already built and cached, and that the
    target registry is already authenticated (e.g., via copy_files docker_login).

    By default, runs in dry-run mode. Use --upload to actually copy images.
    """
    try:
        if not upload:
            logger.info("DRY RUN MODE - no images will be copied")

        rayci_work_repo = rayci_work_repo or os.environ.get("RAYCI_WORK_REPO")
        rayci_build_id = rayci_build_id or os.environ.get("RAYCI_BUILD_ID")
        wanda_image_name = wanda_image_name or os.environ.get("WANDA_IMAGE_NAME")
        target_repo = target_registry or os.environ.get("TARGET_REPO")
        commit_hash = commit_hash or os.environ.get("BUILDKITE_COMMIT")

        required = {
            "RAYCI_WORK_REPO": rayci_work_repo,
            "RAYCI_BUILD_ID": rayci_build_id,
            "WANDA_IMAGE_NAME": wanda_image_name,
            "TARGET_REPO": target_repo,
            "BUILDKITE_COMMIT": commit_hash,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise CopyWandaImageError(f"Missing required values: {', '.join(missing)}")

        source_tag = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"
        target_tag = f"{target_repo}:{commit_hash}"

        logger.info(f"Source tag (Wanda): {source_tag}")
        logger.info(f"Target tag: {target_tag}")

        # Check if target already exists (only in upload mode)
        if upload:
            logger.info("Checking if image already exists at target...")
            if _image_exists(target_tag):
                logger.info(f"Image already exists: {target_tag}")
                logger.info("Nothing to do, exiting successfully")
                return
            logger.info("Image does not exist, proceeding with copy")

        logger.info("Verifying source image in Wanda cache...")
        if not _image_exists(source_tag):
            raise CopyWandaImageError(
                f"Source image not found in Wanda cache: {source_tag}"
            )

        # Copy image
        _copy_image(source_tag, target_tag, dry_run=not upload)

    except CopyWandaImageError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
