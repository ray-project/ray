"""
Push Wanda-cached anyscale images to ECR, GCP, and Azure registries.

This script copies anyscale images from the Wanda cache to the three cloud
registries used for release tests:
- AWS ECR: anyscale/{image_type}:{tag}
- GCP Artifact Registry: anyscale/{image_type}:{tag}
- Azure Container Registry: anyscale/{image_type}:{tag}

Example:
    bazel run //ci/ray_ci/automation:push_anyscale_image -- \\
        --python-version 3.10 \\
        --platform cpu \\
        --image-type ray \\
        --upload

Run with --help to see all options.
"""

import logging
import os
import subprocess
import sys
from typing import List

import click

from ci.ray_ci.automation.crane_lib import (
    call_crane_copy,
    call_crane_manifest,
)
from ci.ray_ci.utils import ecr_docker_login

from ray_release.configs.global_config import get_global_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Registry URLs
_DOCKER_ECR_REPO = os.environ.get(
    "RAYCI_WORK_REPO",
    "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject",
)
_DOCKER_GCP_REGISTRY = os.environ.get(
    "RAYCI_GCP_REGISTRY",
    "us-west1-docker.pkg.dev/anyscale-oss-ci",
)
_DOCKER_AZURE_REGISTRY = os.environ.get(
    "RAYCI_AZURE_REGISTRY",
    "rayreleasetest.azurecr.io",
)
_AZURE_REGISTRY_NAME = "rayreleasetest"


class PushAnyscaleImageError(Exception):
    """Error raised when pushing anyscale images fails."""


def _format_python_version_tag(python_version: str) -> str:
    """Format python version as -py310 (no dots, with hyphen prefix)."""
    return f"-py{python_version.replace('.', '')}"


def _format_platform_tag(platform: str) -> str:
    """Format platform as -cpu or shortened CUDA version."""
    if platform == "cpu":
        return "-cpu"
    # cu12.3.2-cudnn9 -> -cu123
    versions = platform.split(".")
    return f"-{versions[0]}{versions[1]}"


def _get_image_tags(python_version: str, platform: str) -> List[str]:
    """
    Generate image tags matching the original docker_container.py format.

    Returns multiple tags for the image (canonical + aliases).
    """
    branch = os.environ.get("BUILDKITE_BRANCH", "")
    commit = os.environ.get("BUILDKITE_COMMIT", "")[:6]
    rayci_build_id = os.environ.get("RAYCI_BUILD_ID", "")

    py_tag = _format_python_version_tag(python_version)
    platform_tag = _format_platform_tag(platform)

    tags = []

    if branch == "master":
        # On master, use sha and build_id as tags
        tags.append(f"{commit}{py_tag}{platform_tag}")
        if rayci_build_id:
            tags.append(f"{rayci_build_id}{py_tag}{platform_tag}")
    elif branch.startswith("releases/"):
        # On release branches, use release name
        release_name = branch[len("releases/") :]
        tags.append(f"{release_name}.{commit}{py_tag}{platform_tag}")
        if rayci_build_id:
            tags.append(f"{rayci_build_id}{py_tag}{platform_tag}")
    else:
        # For other branches (PRs, etc.)
        pr = os.environ.get("BUILDKITE_PULL_REQUEST", "false")
        if pr != "false":
            tags.append(f"pr-{pr}.{commit}{py_tag}{platform_tag}")
        else:
            tags.append(f"{commit}{py_tag}{platform_tag}")
        if rayci_build_id:
            tags.append(f"{rayci_build_id}{py_tag}{platform_tag}")

    return tags


def _get_wanda_image_name(python_version: str, platform: str, image_type: str) -> str:
    """Get the wanda-cached image name.

    Platform is passed with "cu" prefix (e.g., "cu12.3.2-cudnn9") or "cpu".
    """
    if platform == "cpu":
        return f"{image_type}-anyscale-py{python_version}-cpu"
    else:
        # Platform already includes "cu" prefix from pipeline matrix
        return f"{image_type}-anyscale-py{python_version}-{platform}"


def _run_shell_command(cmd: str, dry_run: bool = False) -> None:
    """Run a shell command."""
    if dry_run:
        logger.info(f"DRY RUN: Would run: {cmd}")
        return

    logger.info(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise PushAnyscaleImageError(f"Command failed: {result.stderr}")


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
        raise PushAnyscaleImageError(f"Crane copy failed: {output}")


@click.command()
@click.option(
    "--python-version",
    type=str,
    required=True,
    help="Python version (e.g., '3.10')",
)
@click.option(
    "--platform",
    type=str,
    required=True,
    help="Platform (e.g., 'cpu', 'cu12.3.2-cudnn9')",
)
@click.option(
    "--image-type",
    type=str,
    default="ray",
    help="Image type (e.g., 'ray', 'ray-llm', 'ray-ml')",
)
@click.option(
    "--upload",
    is_flag=True,
    default=False,
    help="Actually push to registries. Without this flag, runs in dry-run mode.",
)
def main(
    python_version: str,
    platform: str,
    image_type: str,
    upload: bool,
) -> None:
    """
    Push a Wanda-cached anyscale image to ECR, GCP, and Azure registries.

    Handles authentication for all three registries and copies the image
    with appropriate tags.
    """
    dry_run = not upload
    if dry_run:
        logger.info("DRY RUN MODE - no images will be pushed")

    # Get required environment variables
    rayci_work_repo = os.environ.get("RAYCI_WORK_REPO", _DOCKER_ECR_REPO)
    rayci_build_id = os.environ.get("RAYCI_BUILD_ID")

    if not rayci_build_id:
        raise PushAnyscaleImageError("RAYCI_BUILD_ID environment variable not set")

    # Construct source image from Wanda cache
    wanda_image_name = _get_wanda_image_name(python_version, platform, image_type)
    source_tag = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"

    logger.info(f"Source image (Wanda): {source_tag}")

    # Authenticate with ECR (source registry)
    ecr_registry = rayci_work_repo.split("/")[0]
    ecr_docker_login(ecr_registry)

    # Verify source image exists
    logger.info("Verifying source image in Wanda cache...")
    if not _image_exists(source_tag):
        raise PushAnyscaleImageError(
            f"Source image not found in Wanda cache: {source_tag}"
        )

    # Get image tags
    tags = _get_image_tags(python_version, platform)
    canonical_tag = tags[0]

    logger.info(f"Canonical tag: {canonical_tag}")
    logger.info(f"All tags: {tags}")

    # Authenticate with GCP
    gce_credentials = get_global_config()["aws2gce_credentials"]
    _run_shell_command(f"./release/gcloud_docker_login.sh {gce_credentials}", dry_run)

    # Authenticate with Azure
    _run_shell_command("./release/azure_docker_login.sh", dry_run)
    _run_shell_command(f"az acr login --name {_AZURE_REGISTRY_NAME}", dry_run)

    # Push to all registries with all tags
    registries = [
        (ecr_registry, "ECR"),
        (_DOCKER_GCP_REGISTRY, "GCP"),
        (_DOCKER_AZURE_REGISTRY, "Azure"),
    ]

    for tag in tags:
        for registry, name in registries:
            dest_image = f"{registry}/anyscale/{image_type}:{tag}"
            logger.info(f"Pushing to {name}: {dest_image}")
            _copy_image(source_tag, dest_image, dry_run)

    logger.info("Successfully pushed anyscale images to all registries")


if __name__ == "__main__":
    main()
