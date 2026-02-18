"""
Push Wanda-cached release test images to ECR, GCP, and Azure registries used for
release tests:
- AWS ECR: anyscale/{image_type}:{tag}
- GCP Artifact Registry: anyscale/{image_type}:{tag}
- Azure Container Registry: anyscale/{image_type}:{tag}

Example:
    bazel run //ci/ray_ci/automation:push_release_test_image -- \\
        --python-version 3.10 \\
        --platform cpu \\
        --image-type ray \\
        --upload

Run with --help to see all options.
"""

import logging
import subprocess
import sys
from typing import List

import click
import runfiles

from ci.ray_ci.automation.image_tags_lib import (
    ImageTagsError,
    copy_image,
    get_platform_suffixes,
    get_python_suffixes,
    get_variation_suffix,
    image_exists,
)
from ci.ray_ci.container import _AZURE_REGISTRY_NAME
from ci.ray_ci.utils import ci_init, ecr_docker_login

from ray_release.configs.global_config import get_global_config

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_runfiles = runfiles.Create()


class PushReleaseTestImageError(Exception):
    """Error raised when pushing release test images fails."""


def _run_gcloud_docker_login() -> None:
    """Authenticate with GCP Artifact Registry using gcloud."""
    credentials_path = _runfiles.Rlocation("io_ray/release/aws2gce_iam.json")

    logger.info("Authenticating with GCP Artifact Registry...")

    result = subprocess.run(
        ["gcloud", "auth", "login", "--cred-file", credentials_path, "--quiet"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise PushReleaseTestImageError(f"GCP auth login failed: {result.stderr}")

    gcp_registry = get_global_config()["byod_gcp_cr"]
    gcp_hostname = gcp_registry.split("/")[0]
    result = subprocess.run(
        ["gcloud", "auth", "configure-docker", gcp_hostname, "--quiet"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise PushReleaseTestImageError(f"GCP docker config failed: {result.stderr}")


def _run_azure_docker_login() -> None:
    """Authenticate with Azure Container Registry."""
    script_path = _runfiles.Rlocation("io_ray/release/azure_docker_login.sh")

    logger.info("Authenticating with Azure Container Registry...")
    result = subprocess.run(
        ["bash", script_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise PushReleaseTestImageError(f"Azure authentication failed: {result.stderr}")

    logger.info(f"Logging into Azure ACR: {_AZURE_REGISTRY_NAME}...")
    result = subprocess.run(
        ["az", "acr", "login", "--name", _AZURE_REGISTRY_NAME],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise PushReleaseTestImageError(f"Azure ACR login failed: {result.stderr}")


class ReleaseTestImagePushContext:
    """Context for publishing an anyscale image from Wanda cache to cloud registries."""

    image_type: str
    python_version: str
    platform: str
    branch: str
    commit: str
    rayci_build_id: str
    pull_request: str
    # Computed fields (set in __init__)
    wanda_tag: str

    def __init__(
        self,
        image_type: str,
        python_version: str,
        platform: str,
        branch: str,
        commit: str,
        rayci_build_id: str,
        pull_request: str,
    ) -> None:
        self.image_type = image_type
        self.python_version = python_version
        self.platform = platform
        self.branch = branch
        self.commit = commit
        self.rayci_build_id = rayci_build_id
        self.pull_request = pull_request

        self.wanda_tag = f"{rayci_build_id}-{self.wanda_image_name()}"

    def destination_tags(self) -> List[str]:
        """
        Compute the destination tags for this context.

        Tags are formed as:
        {version}{variation}{python_suffix}{platform_suffix}

        For example:
        - abc123-py310-cpu
        - abc123-py310-gpu
        - abc123-py310
        - 2.53.0.abc123-py310-cu121
        """
        tags = []
        for version in self._versions():
            for plat in self._platform_suffixes():
                for py in self._python_suffixes():
                    tags.append(f"{version}{self._variation_suffix()}{py}{plat}")
        return tags

    def wanda_image_name(self) -> str:
        """Get the wanda source image name for this context."""
        return f"{self.image_type}-anyscale-py{self.python_version}-{self.platform}"

    def _versions(self) -> List[str]:
        """Compute version tags based on branch/PR status.

        Priority matches original DockerContainer._get_image_version_tags:
        1. master branch -> sha_tag
        2. release branch -> release_version.sha_tag
        3. PR -> pr-{number}.sha_tag
        4. other branches -> sha_tag
        """
        sha_tag = self.commit[:6]

        if self.branch == "master":
            primary_tag = sha_tag
        elif self.branch and self.branch.startswith("releases/"):
            primary_tag = f"{self.branch[len('releases/'):]}.{sha_tag}"
        elif self.pull_request != "false":
            primary_tag = f"pr-{self.pull_request}.{sha_tag}"
        else:
            primary_tag = sha_tag

        versions = [primary_tag]
        if self.rayci_build_id:
            versions.append(self.rayci_build_id)

        return versions

    def _variation_suffix(self) -> str:
        """Get -extra suffix for extra image types."""
        return get_variation_suffix(self.image_type)

    def _python_suffixes(self) -> List[str]:
        """Get python version suffixes (includes empty for default version)."""
        return get_python_suffixes(self.python_version)

    def _platform_suffixes(self) -> List[str]:
        """Get platform suffixes (includes aliases like -gpu for GPU_PLATFORM)."""
        return get_platform_suffixes(self.platform, self.image_type)


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
@click.option(
    "--rayci-work-repo",
    type=str,
    required=True,
    envvar="RAYCI_WORK_REPO",
    help="ECR work repo (e.g., '029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp')",
)
@click.option(
    "--rayci-build-id",
    type=str,
    required=True,
    envvar="RAYCI_BUILD_ID",
    help="Rayci build ID",
)
@click.option("--branch", type=str, required=True, envvar="BUILDKITE_BRANCH")
@click.option("--commit", type=str, required=True, envvar="BUILDKITE_COMMIT")
@click.option(
    "--pull-request", type=str, default="false", envvar="BUILDKITE_PULL_REQUEST"
)
def main(
    python_version: str,
    platform: str,
    image_type: str,
    upload: bool,
    rayci_work_repo: str,
    rayci_build_id: str,
    branch: str,
    commit: str,
    pull_request: str,
) -> None:
    """
    Push a Wanda-cached release test image to ECR, GCP, and Azure registries.

    Handles authentication for all three registries internally.
    """
    ci_init()

    dry_run = not upload
    if dry_run:
        logger.info("DRY RUN MODE - no images will be pushed")

    ctx = ReleaseTestImagePushContext(
        image_type=image_type,
        python_version=python_version,
        platform=platform,
        branch=branch,
        commit=commit,
        rayci_build_id=rayci_build_id,
        pull_request=pull_request,
    )

    ecr_registry = rayci_work_repo.split("/")[0]
    source_tag = f"{rayci_work_repo}:{ctx.wanda_tag}"
    logger.info(f"Source image (Wanda): {source_tag}")

    # Get image tags
    try:
        tags = ctx.destination_tags()
    except ImageTagsError as e:
        raise PushReleaseTestImageError(str(e))

    canonical_tag = tags[0]
    logger.info(f"Canonical tag: {canonical_tag}")
    logger.info(f"All tags: {tags}")

    # Destination registries (from global config)
    global_config = get_global_config()
    registries = [
        (ecr_registry, "ECR"),
        (global_config["byod_gcp_cr"], "GCP"),
        (global_config["byod_azure_cr"], "Azure"),
    ]

    if dry_run:
        for tag in tags:
            for registry, name in registries:
                dest_image = f"{registry}/anyscale/{image_type}:{tag}"
                logger.info(f"Would push to {name}: {dest_image}")
        return

    # Authenticate with all registries
    ecr_docker_login(ecr_registry)
    _run_gcloud_docker_login()
    _run_azure_docker_login()

    # Verify source image exists
    logger.info("Verifying source image in Wanda cache...")
    if not image_exists(source_tag):
        raise PushReleaseTestImageError(
            f"Source image not found in Wanda cache: {source_tag}"
        )

    # Push to all three registries
    try:
        for tag in tags:
            for registry, name in registries:
                dest_image = f"{registry}/anyscale/{image_type}:{tag}"
                logger.info(f"Pushing to {name}: {dest_image}")
                copy_image(source_tag, dest_image, dry_run=False)
    except ImageTagsError as e:
        raise PushReleaseTestImageError(str(e))

    logger.info("Successfully pushed release test images to all registries")


if __name__ == "__main__":
    main()
