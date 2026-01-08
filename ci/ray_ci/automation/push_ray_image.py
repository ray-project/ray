"""
Push Wanda-cached ray images to Docker Hub.

This script copies ray images from the Wanda cache to Docker Hub with tags
matching the original format from docker_container.py.

Supports multiple image types:
    - ray: Standard ray image -> rayproject/ray
    - ray-extra: Ray with extra deps -> rayproject/ray
    - ray-llm: Ray for LLM workloads -> rayproject/ray-llm
    - ray-llm-extra: Ray LLM with extra deps -> rayproject/ray-llm

Example:
    bazel run //ci/ray_ci/automation:push_ray_image -- \\
        --python-version 3.10 \\
        --platform cpu \\
        --image-type ray \\
        --upload

Tag format:
    - Nightly: nightly.YYMMDD.{sha[:6]}-py310-cpu
    - Release: {release_name}.{sha[:6]}-py310-cpu
    - Other: {sha[:6]}-py310-cpu

Run with --help to see all options.
"""

import logging
import os
import sys
from datetime import datetime, timezone as tz
from typing import List

import click

from ci.ray_ci.automation.crane_lib import (
    call_crane_copy,
    call_crane_manifest,
)
from ci.ray_ci.docker_container import RAY_REPO_MAP
from ci.ray_ci.utils import ecr_docker_login

# GPU_PLATFORM is the default GPU platform that gets aliased as "gpu"
# This must match the definition in ci/ray_ci/docker_container.py
GPU_PLATFORM = "cu12.1.1-cudnn8"

# Default architecture (x86_64 gets no suffix)
DEFAULT_ARCHITECTURE = "x86_64"

# Valid image types that can be pushed
VALID_IMAGE_TYPES = list(RAY_REPO_MAP.keys())

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class PushRayImageError(Exception):
    """Error raised when pushing ray images fails."""


def _format_python_version_tag(python_version: str) -> str:
    """
    Format python version as -py310 (no dots, with hyphen prefix).

    Examples:
        3.10 -> -py310
        3.11 -> -py311
    """
    return f"-py{python_version.replace('.', '')}"


def _format_platform_tag(platform: str) -> str:
    """
    Format platform as -cpu or shortened CUDA version.

    Examples:
        cpu -> -cpu
        cu11.7.1-cudnn8 -> -cu117
        cu12.1.1-cudnn8 -> -cu121
    """
    if platform == "cpu":
        return "-cpu"
    # cu11.7.1-cudnn8 -> ['cu11', '7', '1-cudnn8'] -> -cu117
    versions = platform.split(".")
    return f"-{versions[0]}{versions[1]}"


def _format_architecture_tag(architecture: str) -> str:
    """
    Format architecture as suffix (empty for x86_64, -aarch64 for aarch64).

    Examples:
        x86_64 -> ""
        aarch64 -> -aarch64
    """
    if architecture == DEFAULT_ARCHITECTURE:
        return ""
    return f"-{architecture}"


def _generate_image_tags(
    commit: str,
    python_version: str,
    platform: str,
    architecture: str = DEFAULT_ARCHITECTURE,
) -> List[str]:
    """
    Generate destination tags matching the original ray docker image format.

    Tag format:
        {version_prefix}{py_tag}{platform_tag}{arch_tag}

    Version prefix:
        - Nightly (master + nightly schedule): nightly.YYMMDD.{sha[:6]}
        - Release branches: {release_name}.{sha[:6]}
        - Other: {sha[:6]}

    For GPU_PLATFORM, also generates -gpu alias tags.
    """
    branch = os.environ.get("BUILDKITE_BRANCH", "")
    schedule = os.environ.get("RAYCI_SCHEDULE", "")

    sha_tag = commit[:6]
    formatted_date = datetime.now(tz.utc).strftime("%y%m%d")

    # Generate version prefix
    if branch == "master" and schedule == "nightly":
        version_tags = [f"nightly.{formatted_date}.{sha_tag}"]
    elif branch.startswith("releases/"):
        release_name = branch[len("releases/") :]
        version_tags = [f"{release_name}.{sha_tag}"]
    else:
        version_tags = [sha_tag]

    py_tag = _format_python_version_tag(python_version)
    arch_tag = _format_architecture_tag(architecture)

    # For GPU_PLATFORM, also create -gpu alias
    platform_tags = [_format_platform_tag(platform)]
    if platform == GPU_PLATFORM:
        platform_tags.append("-gpu")

    tags = []
    for version in version_tags:
        for ptag in platform_tags:
            tags.append(f"{version}{py_tag}{ptag}{arch_tag}")

    return tags


def _get_wanda_image_name(
    image_type: str,
    python_version: str,
    platform: str,
    architecture: str = DEFAULT_ARCHITECTURE,
) -> str:
    """
    Get the wanda-cached image name for the given image type.

    Wanda image naming follows the pattern:
        {image_type}-py{version}-{platform}{arch_suffix}

    Examples:
        ray-py3.10-cpu
        ray-extra-py3.10-cu12.1.1-cudnn8
        ray-llm-py3.11-cu12.8.1-cudnn
    """
    arch_suffix = _format_architecture_tag(architecture)
    if platform == "cpu":
        return f"{image_type}-py{python_version}-cpu{arch_suffix}"
    else:
        return f"{image_type}-py{python_version}-{platform}{arch_suffix}"


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
        raise PushRayImageError(f"Crane copy failed: {output}")
    logger.info(f"Successfully copied to {destination}")


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
    help="Platform (e.g., 'cpu', 'cu11.7.1-cudnn8')",
)
@click.option(
    "--image-type",
    type=click.Choice(VALID_IMAGE_TYPES),
    default="ray",
    help="Image type (e.g., 'ray', 'ray-extra', 'ray-llm', 'ray-llm-extra')",
)
@click.option(
    "--architecture",
    type=str,
    default=DEFAULT_ARCHITECTURE,
    help="Architecture (e.g., 'x86_64', 'aarch64')",
)
@click.option(
    "--upload",
    is_flag=True,
    default=False,
    help="Actually push to Docker Hub. Without this flag, runs in dry-run mode.",
)
def main(
    python_version: str,
    platform: str,
    image_type: str,
    architecture: str,
    upload: bool,
) -> None:
    """
    Push a Wanda-cached ray image to Docker Hub.

    Handles authentication for ECR (source/Wanda cache) and Docker Hub
    (destination via copy_files.py).

    Supports multiple image types which map to Docker Hub repos:
    - ray, ray-extra -> rayproject/ray
    - ray-llm, ray-llm-extra -> rayproject/ray-llm
    - ray-ml, ray-ml-extra -> rayproject/ray-ml

    Tags are generated matching the original docker_container.py format:
    - Nightly: nightly.YYMMDD.{sha[:6]}-py310-cpu
    - Release: {release_name}.{sha[:6]}-py310-cpu

    For GPU_PLATFORM (cu12.1.1-cudnn8), also pushes with -gpu alias tag.
    """
    dry_run = not upload
    if dry_run:
        logger.info("DRY RUN MODE - no images will be pushed")

    # Get required environment variables
    rayci_work_repo = os.environ.get("RAYCI_WORK_REPO")
    rayci_build_id = os.environ.get("RAYCI_BUILD_ID")
    commit = os.environ.get("BUILDKITE_COMMIT")

    required = {
        "RAYCI_WORK_REPO": rayci_work_repo,
        "RAYCI_BUILD_ID": rayci_build_id,
        "BUILDKITE_COMMIT": commit,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise PushRayImageError(f"Missing required env vars: {', '.join(missing)}")

    # Determine destination Docker Hub repository from image type
    docker_hub_repo = f"rayproject/{RAY_REPO_MAP[image_type]}"
    logger.info(f"Image type: {image_type} -> Docker Hub repo: {docker_hub_repo}")

    # Construct source image from Wanda cache
    wanda_image_name = _get_wanda_image_name(
        image_type, python_version, platform, architecture
    )
    source_tag = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"

    # Generate destination tags (may include aliases like -gpu for GPU_PLATFORM)
    destination_tags = _generate_image_tags(
        commit, python_version, platform, architecture
    )

    logger.info(f"Source image (Wanda): {source_tag}")
    logger.info(f"Destination tags: {destination_tags}")

    # Authenticate with ECR (source registry)
    # Docker Hub auth is handled by copy_files.py --destination docker_login
    ecr_registry = rayci_work_repo.split("/")[0]
    ecr_docker_login(ecr_registry)

    # Verify source image exists
    logger.info("Verifying source image in Wanda cache...")
    if not _image_exists(source_tag):
        raise PushRayImageError(f"Source image not found in Wanda cache: {source_tag}")

    # Copy image to Docker Hub with all tags
    for tag in destination_tags:
        full_destination = f"{docker_hub_repo}:{tag}"
        _copy_image(source_tag, full_destination, dry_run)

    logger.info(f"Successfully pushed {image_type} image with tags: {destination_tags}")


if __name__ == "__main__":
    main()
