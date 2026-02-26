"""
Shared utilities for generating Docker image tags for Ray and Anyscale images.
"""

import logging
from typing import List

from ci.ray_ci.automation.crane_lib import (
    CraneError,
    call_crane_copy,
    call_crane_manifest,
)
from ci.ray_ci.configs import DEFAULT_PYTHON_TAG_VERSION
from ci.ray_ci.docker_container import GPU_PLATFORM

logger = logging.getLogger(__name__)


class ImageTagsError(Exception):
    """Error raised when image tag operations fail."""


def format_platform_tag(platform: str) -> str:
    """
    Format platform as -cpu or shortened CUDA version.

    Examples:
        cpu -> -cpu
        cu12.1.1-cudnn8 -> -cu121
        cu12.3.2-cudnn9 -> -cu123
    """
    if platform == "cpu":
        return "-cpu"
    # cu12.3.2-cudnn9 -> -cu123
    platform_base = platform.split("-", 1)[0]
    parts = platform_base.split(".")
    if len(parts) < 2:
        raise ImageTagsError(f"Unrecognized GPU platform format: {platform}")
    return f"-{parts[0]}{parts[1]}"


def format_python_tag(python_version: str) -> str:
    """
    Format python version as -py310 (no dots, with hyphen prefix).

    Examples:
        3.10 -> -py310
        3.11 -> -py311
    """
    return f"-py{python_version.replace('.', '')}"


def get_python_suffixes(python_version: str) -> List[str]:
    """
    Get python version suffixes (includes empty for default version).

    For DEFAULT_PYTHON_TAG_VERSION (3.10), returns both the explicit
    suffix and an empty suffix for backward compatibility.

    Examples:
        3.10 -> ["-py310", ""]
        3.11 -> ["-py311"]
    """
    suffixes = [format_python_tag(python_version)]
    if python_version == DEFAULT_PYTHON_TAG_VERSION:
        suffixes.append("")
    return suffixes


def get_platform_suffixes(platform: str, image_type: str) -> List[str]:
    """
    Get platform suffixes (includes aliases like -gpu for GPU_PLATFORM).

    Handles the following cases:
    - CPU with ray/ray-extra: adds empty suffix alias
    - GPU_PLATFORM: adds -gpu alias
    - GPU_PLATFORM with ray-ml/ray-ml-extra: adds empty suffix alias

    Args:
        platform: The platform string (e.g., "cpu", "cu12.1.1-cudnn8")
        image_type: The image type (e.g., "ray", "ray-ml", "ray-extra")

    Returns:
        List of platform suffixes to use for tagging.
    """
    platform_tag = format_platform_tag(platform)
    suffixes = [platform_tag]

    if platform == "cpu":
        # no tag is alias to cpu for ray image
        if image_type in ("ray", "ray-extra"):
            suffixes.append("")
    elif platform == GPU_PLATFORM:
        # gpu is alias to GPU_PLATFORM
        suffixes.append("-gpu")
        # no tag is alias to gpu for ray-ml image
        if image_type in ("ray-ml", "ray-ml-extra"):
            suffixes.append("")

    return suffixes


def get_variation_suffix(image_type: str) -> str:
    """
    Get variation suffix for -extra image types.

    Examples:
        ray -> ""
        ray-extra -> "-extra"
        ray-ml-extra -> "-extra"
    """
    if image_type in ("ray-extra", "ray-ml-extra", "ray-llm-extra"):
        return "-extra"
    return ""


def image_exists(tag: str) -> bool:
    """Check if a container image manifest exists using crane."""
    try:
        call_crane_manifest(tag)
        return True
    except CraneError:
        return False


def copy_image(source: str, destination: str, dry_run: bool = False) -> None:
    """Copy a container image from source to destination using crane."""
    if dry_run:
        logger.info(f"DRY RUN: Would copy {source} -> {destination}")
        return

    logger.info(f"Copying {source} -> {destination}")
    try:
        call_crane_copy(source, destination)
        logger.info(f"Successfully copied to {destination}")
    except CraneError as e:
        raise ImageTagsError(f"Crane copy failed: {e}")
