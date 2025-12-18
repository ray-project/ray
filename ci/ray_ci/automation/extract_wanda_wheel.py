"""
Extract Ray wheels from Wanda-cached Docker images.

This script pulls wheel scratch images from the Wanda cache and extracts
the .whl files to the .whl/ directory for upload.

Example:
    bazel run //ci/ray_ci/automation:extract_wanda_wheel -- \\
        --python-version 3.10 \\
        --arch x86_64

Run with --help to see all options.
"""

import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

import click

from ci.ray_ci.utils import docker_pull, ecr_docker_login

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class WheelExtractionError(Exception):
    """Error raised when wheel extraction fails."""


def _get_arch_suffix(arch: str) -> str:
    """Get the architecture suffix for Wanda image names."""
    if arch == "x86_64":
        return ""
    elif arch == "aarch64":
        return "-aarch64"
    else:
        raise WheelExtractionError(f"Unknown architecture: {arch}")


def _extract_wheel(image_tag: str, image_name: str, output_dir: Path) -> None:
    """Extract wheel files from a scratch Docker image."""
    logger.info(f"--- Extracting from {image_name} ---")

    # Pull the image
    try:
        docker_pull(image_tag)
    except subprocess.CalledProcessError as e:
        raise WheelExtractionError(f"Failed to pull image {image_tag}: {e}") from e

    # Create a container from the image
    logger.info(f"Creating container from {image_tag}...")
    result = subprocess.run(
        ["docker", "create", image_tag, "true"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise WheelExtractionError(f"Failed to create container: {result.stderr}")

    container_id = result.stdout.strip()
    logger.info(f"Extracting wheels from container {container_id}...")

    try:
        # Extract files to a temp directory first
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Copy root filesystem from container
            result = subprocess.run(
                ["docker", "cp", f"{container_id}:/", str(temp_path)],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise WheelExtractionError(
                    f"Failed to copy from container: {result.stderr}"
                )

            # Find and copy .whl files to output directory
            wheel_count = 0
            for whl_file in temp_path.rglob("*.whl"):
                dest = output_dir / whl_file.name
                shutil.copy2(whl_file, dest)
                logger.info(f"  Extracted: {whl_file.name}")
                wheel_count += 1

            if wheel_count == 0:
                logger.warning(f"  No wheel files found in {image_name}")
            else:
                logger.info(f"Extracted {wheel_count} wheel(s) from {image_name}")

    finally:
        # Clean up container
        subprocess.run(
            ["docker", "rm", container_id],
            capture_output=True,
        )


@click.command()
@click.option(
    "--python-version",
    type=str,
    required=True,
    help="Python version (e.g., '3.10', '3.11', '3.12').",
)
@click.option(
    "--arch",
    type=str,
    required=True,
    help="Architecture (e.g., 'x86_64', 'aarch64').",
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
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path(".whl"),
    help="Output directory for extracted wheels. Defaults to '.whl'.",
)
@click.option(
    "--skip-ray",
    is_flag=True,
    default=False,
    help="Skip extracting the ray wheel.",
)
@click.option(
    "--skip-ray-cpp",
    is_flag=True,
    default=False,
    help="Skip extracting the ray-cpp wheel.",
)
def main(
    python_version: str,
    arch: str,
    rayci_work_repo: Optional[str],
    rayci_build_id: Optional[str],
    output_dir: Path,
    skip_ray: bool,
    skip_ray_cpp: bool,
) -> None:
    """
    Extract Ray wheels from Wanda-cached Docker images.

    Pulls the wheel scratch images from the Wanda cache and extracts
    the .whl files to the output directory.
    """
    # Get configuration from env vars if not provided
    rayci_work_repo = rayci_work_repo or os.environ.get(
        "RAYCI_WORK_REPO",
        "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp",
    )
    rayci_build_id = rayci_build_id or os.environ.get("RAYCI_BUILD_ID")

    if not rayci_build_id:
        raise WheelExtractionError("RAYCI_BUILD_ID environment variable required")

    # Determine architecture suffix
    arch_suffix = _get_arch_suffix(arch)

    # Build image names
    ray_wheel_image = f"ray-wheel-py{python_version}{arch_suffix}"
    ray_cpp_wheel_image = f"ray-cpp-wheel-py{python_version}{arch_suffix}"

    # Full image tags in Wanda cache
    ray_wheel_tag = f"{rayci_work_repo}:{rayci_build_id}-{ray_wheel_image}"
    ray_cpp_wheel_tag = f"{rayci_work_repo}:{rayci_build_id}-{ray_cpp_wheel_image}"

    logger.info("=== Extracting Ray wheels from Wanda cache ===")
    logger.info(f"Python version: {python_version}")
    logger.info(f"Architecture: {arch} (suffix: '{arch_suffix}')")
    logger.info(f"Ray wheel image: {ray_wheel_tag}")
    logger.info(f"Ray C++ wheel image: {ray_cpp_wheel_tag}")
    logger.info(f"Output directory: {output_dir}")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Authenticate with ECR
    ecr_registry = rayci_work_repo.split("/")[0]
    ecr_docker_login(ecr_registry)

    # Extract wheels
    if not skip_ray:
        _extract_wheel(ray_wheel_tag, ray_wheel_image, output_dir)

    if not skip_ray_cpp:
        _extract_wheel(ray_cpp_wheel_tag, ray_cpp_wheel_image, output_dir)

    # List extracted wheels
    logger.info("=== Extraction complete ===")
    logger.info("Wheels in output directory:")
    wheels = list(output_dir.glob("*.whl"))
    if wheels:
        for whl in sorted(wheels):
            size_mb = whl.stat().st_size / (1024 * 1024)
            logger.info(f"  {whl.name} ({size_mb:.1f} MB)")
    else:
        logger.warning("  (no wheel files found)")


if __name__ == "__main__":
    main()
