import logging
import os
import shutil
import sys
import tempfile
from datetime import datetime
from typing import List

import click

from ci.ray_ci.automation.crane_lib import CraneError, call_crane_export
from ci.ray_ci.automation.image_tags_lib import (
    ImageTagsError,
    copy_image,
    format_platform_tag,
    get_platform_suffixes,
    get_python_suffixes,
    image_exists,
)
from ci.ray_ci.configs import (
    ARCHITECTURE,
    PYTHON_VERSIONS,
)
from ci.ray_ci.docker_container import (
    PLATFORMS_RAY,
    RayType,
)
from ci.ray_ci.ray_image import IMAGE_TYPE_CONFIG, RayImage, RayImageError
from ci.ray_ci.utils import ci_init, ecr_docker_login

from ray_release.configs.global_config import get_global_config

VALID_IMAGE_TYPES = list(IMAGE_TYPE_CONFIG.keys())

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# rayci auto-uploads everything under /artifact-mount as Buildkite artifacts.
# Mirrors the old ci/build/build-ray-docker.sh staging location so the artifact
# filename/layout is unchanged for downstream consumers.
ARTIFACT_MOUNT_IMAGE_INFO_DIR = "/artifact-mount/.image-info"
# Path of the pip freeze inside the image filesystem (no leading slash in the
# crane-exported tar). Written by ci/docker/ray-image.Dockerfile.
PIP_FREEZE_PATH_IN_IMAGE = os.path.join("home", "ray", "pip-freeze.txt")


class PushRayImageError(Exception):
    """Error raised when pushing ray images fails."""


# Re-export for backward compatibility with tests
def compact_cuda_suffix(platform: str) -> str:
    """Convert a CUDA platform string to compact suffix (e.g. cu12.1.1-cudnn8 -> -cu121)."""
    return format_platform_tag(platform)


class RayImagePushContext:
    """Context for publishing a ray image from Wanda cache to Docker Hub."""

    ray_type: RayType
    python_version: str
    platform: str
    architecture: str
    branch: str
    commit: str
    rayci_schedule: str
    rayci_build_id: str
    pull_request: str  # buildkite uses "false" or number string
    # Computed fields (set in __init__)
    arch_suffix: str
    wanda_tag: str
    docker_hub_repo: str

    def __init__(
        self,
        ray_type: RayType,
        python_version: str,
        platform: str,
        architecture: str,
        branch: str,
        commit: str,
        rayci_schedule: str,
        rayci_build_id: str,
        pull_request: str,
    ) -> None:
        self.ray_type = ray_type
        self.python_version = python_version
        self.platform = platform
        self.architecture = architecture
        self.branch = branch
        self.commit = commit
        self.rayci_schedule = rayci_schedule
        self.rayci_build_id = rayci_build_id
        self.pull_request = pull_request

        self.ray_image = RayImage(
            image_type=ray_type.value,
            python_version=python_version,
            platform=platform,
            architecture=architecture,
        )
        self.arch_suffix = self.ray_image.arch_suffix
        self.wanda_tag = f"{rayci_build_id}-{self.wanda_image_name()}"
        self.docker_hub_repo = f"rayproject/{self.ray_image.repo}"

    def assert_published_image_type(self) -> None:
        try:
            self.ray_image.validate()
        except RayImageError as e:
            raise PushRayImageError(str(e)) from e

    def destination_tags(self) -> List[str]:
        """
        Compute the destination tags for this context.

        Tags are formed as:
        {version}{variation}{python_suffix}{platform}{architecture_suffix}

        For example:
        - nightly.260107.abc123-py310-cpu
        - nightly-extra-py310-cu121
        - nightly.260107.abc123-extra-py310-gpu
        - 2.53.0.abc123-py310-cu121
        - 2.53.0.abc123-extra-py310-cu121
        """
        tags = []
        for version in self._versions():
            for plat in self._platform_suffixes():
                for py in self._python_suffixes():
                    tags.append(
                        f"{version}{self._variation_suffix()}{py}{plat}{self.arch_suffix}"
                    )
        return tags

    def _versions(self) -> List[str]:
        """Compute version tags based on branch/schedule/PR status."""
        is_master = self.branch == "master"
        is_nightly = self.rayci_schedule == "nightly"
        is_pull_request = self.pull_request != "false"
        is_release = self.branch and self.branch.startswith("releases/")
        sha_tag = self.commit[:6]
        formatted_date = datetime.now().strftime("%y%m%d")

        if is_master:
            if is_nightly:
                return [f"nightly.{formatted_date}.{sha_tag}", "nightly"]
            return [sha_tag, self.rayci_build_id]
        elif is_release:
            release_name = self.branch[len("releases/") :]
            return [f"{release_name}.{sha_tag}"]
        elif is_pull_request:
            return [f"pr-{self.pull_request}.{sha_tag}", self.rayci_build_id]
        else:
            return [sha_tag, self.rayci_build_id]

    def wanda_image_name(self) -> str:
        """Get the wanda source image name for this context."""
        return self.ray_image.wanda_image_name

    def _variation_suffix(self) -> str:
        """Get -extra suffix for extra image types."""
        return self.ray_image.variation_suffix

    def _python_suffixes(self) -> List[str]:
        """Get python version suffixes (includes empty for default version)."""
        return get_python_suffixes(self.python_version)

    def _platform_suffixes(self) -> List[str]:
        """Get platform suffixes (includes aliases like -gpu for GPU_PLATFORM)."""
        return get_platform_suffixes(self.platform, self.ray_type.value)

    def pip_freeze_canonical_tag(self) -> str:
        """
        Canonical image tag used to name the pip-freeze Buildkite artifact.

        Mirrors the old RayDockerContainer._get_image_tags(external=False)[0]
        contract: the *internal* version tag (bare "<sha6>" on master/PR/other,
        "<release_name>.<sha6>" on releases/* branches) -- NOT the external
        "nightly.*" tag. Format: "<version><variation>-py<NN>-cpu<arch_suffix>".

        This is the source-of-truth filename consumed by the anyscale/product
        repo at devprod/rayrelease/releaser.py:
        update_doc_with_latest_docker_dependencies. Keep the two in sync.
        """
        sha = self.commit[:6]
        if self.branch and self.branch.startswith("releases/"):
            version = f"{self.branch[len('releases/'):]}.{sha}"
        else:
            version = sha
        py_tag = f"-py{self.python_version.replace('.', '')}"
        return f"{version}{self._variation_suffix()}{py_tag}-cpu{self.arch_suffix}"

    def pip_freeze_artifact_filename(self) -> str:
        """Buildkite artifact basename: "<image_type>:<canonical_tag>_pip-freeze.txt"."""
        return (
            f"{self.ray_image.image_type}:"
            f"{self.pip_freeze_canonical_tag()}_pip-freeze.txt"
        )


def _image_exists(tag: str) -> bool:
    """Check if a container image manifest exists using crane."""
    return image_exists(tag)


def _copy_image(reference: str, destination: str, dry_run: bool = False) -> None:
    """Copy a container image from source to destination using crane."""
    try:
        copy_image(reference, destination, dry_run)
    except ImageTagsError as e:
        raise PushRayImageError(str(e))


def _export_pip_freeze(src_ref: str, ctx: RayImagePushContext) -> str:
    """
    Export the image's pip-freeze.txt and stage it as a Buildkite artifact.

    Uses `crane export` (no docker daemon) to pull the image filesystem, reads
    PIP_FREEZE_PATH_IN_IMAGE out of it, and copies it under
    ARTIFACT_MOUNT_IMAGE_INFO_DIR using ctx.pip_freeze_artifact_filename().

    Runs independently of whether images are pushed (dry_run), so the shipped
    dependencies record is produced even on PR/dry-run builds -- mirroring the
    old ci/build/build-ray-docker.sh behavior.

    Returns the staged file path.

    Raises:
        PushRayImageError: if the image has no pip-freeze.txt, or if the crane
            export / filesystem staging fails (CraneError / OSError are wrapped,
            mirroring _copy_image's handling of ImageTagsError).
    """
    try:
        with tempfile.TemporaryDirectory() as export_dir:
            logger.info(f"Exporting pip freeze from {src_ref}")
            call_crane_export(src_ref, export_dir)

            freeze_src = os.path.join(export_dir, PIP_FREEZE_PATH_IN_IMAGE)
            if not os.path.exists(freeze_src):
                raise PushRayImageError(
                    f"pip-freeze.txt not found in image {src_ref} "
                    f"at /{PIP_FREEZE_PATH_IN_IMAGE}"
                )

            os.makedirs(ARTIFACT_MOUNT_IMAGE_INFO_DIR, exist_ok=True)
            dest = os.path.join(
                ARTIFACT_MOUNT_IMAGE_INFO_DIR, ctx.pip_freeze_artifact_filename()
            )
            shutil.copyfile(freeze_src, dest)
            logger.info(f"Staged pip freeze Buildkite artifact at {dest}")
            return dest
    except (CraneError, OSError) as e:
        raise PushRayImageError(
            f"Failed to export pip-freeze from {src_ref}: {e}"
        ) from e


def _should_upload(pipeline_id: str, branch: str, rayci_schedule: str) -> bool:
    """
    Check if upload should proceed based on pipeline and branch context.

    Mirrors the logic from RayDockerContainer._should_upload() to prevent
    accidental pushes from feature branches or non-postmerge pipelines.

    Returns True only if:
    - Pipeline is a postmerge pipeline AND
    - Branch is releases/* OR (branch is master AND schedule is nightly)
    """
    postmerge_pipelines = get_global_config()["ci_pipeline_postmerge"]
    if pipeline_id not in postmerge_pipelines:
        logger.info(
            f"Pipeline {pipeline_id} is not a postmerge pipeline, skipping upload"
        )
        return False

    if branch.startswith("releases/"):
        return True

    if branch == "master" and rayci_schedule == "nightly":
        return True

    logger.info(
        f"Branch '{branch}' with schedule '{rayci_schedule}' is not eligible for upload. "
        "Upload is only allowed for releases/* branches or master with nightly schedule."
    )
    return False


@click.command()
@click.option(
    "--python-version", type=click.Choice(list(PYTHON_VERSIONS.keys())), required=True
)
@click.option(
    "--platform",
    type=click.Choice(list(PLATFORMS_RAY)),
    required=True,
    multiple=True,
    help="Platform(s) to push. Can be specified multiple times.",
)
@click.option(
    "--image-type",
    type=click.Choice(VALID_IMAGE_TYPES),
    required=True,
)
@click.option("--architecture", type=click.Choice(ARCHITECTURE), required=True)
@click.option("--rayci-work-repo", type=str, required=True, envvar="RAYCI_WORK_REPO")
@click.option("--rayci-build-id", type=str, required=True, envvar="RAYCI_BUILD_ID")
@click.option("--pipeline-id", type=str, required=True, envvar="BUILDKITE_PIPELINE_ID")
@click.option("--branch", type=str, required=True, envvar="BUILDKITE_BRANCH")
@click.option("--commit", type=str, required=True, envvar="BUILDKITE_COMMIT")
@click.option("--rayci-schedule", type=str, default="", envvar="RAYCI_SCHEDULE")
@click.option(
    "--pull-request", type=str, default="false", envvar="BUILDKITE_PULL_REQUEST"
)
def main(
    python_version: str,
    platform: tuple,
    image_type: str,
    architecture: str,
    rayci_work_repo: str,
    rayci_build_id: str,
    pipeline_id: str,
    branch: str,
    commit: str,
    rayci_schedule: str,
    pull_request: str,
) -> None:
    """
    Publish Wanda-cached ray image(s) to Docker Hub.

    Tags are generated matching the original RayDockerContainer format:
    {version}{variation}{python_suffix}{platform}{architecture_suffix}

    Multiple platforms can be specified to push in a single invocation.
    """
    ci_init()

    dry_run = not _should_upload(pipeline_id, branch, rayci_schedule)
    if dry_run:
        logger.info(
            "DRY RUN MODE - upload conditions not met, no images will be pushed"
        )

    platforms = list(platform)
    logger.info(f"Processing {len(platforms)} platform(s): {platforms}")

    ecr_registry = rayci_work_repo.split("/")[0]
    ecr_docker_login(ecr_registry)

    all_tags = []
    for plat in platforms:
        logger.info(f"\n{'='*60}\nProcessing platform: {plat}\n{'='*60}")

        ctx = RayImagePushContext(
            ray_type=RayType(image_type),
            python_version=python_version,
            platform=plat,
            architecture=architecture,
            branch=branch,
            commit=commit,
            rayci_schedule=rayci_schedule,
            rayci_build_id=rayci_build_id,
            pull_request=pull_request,
        )

        ctx.assert_published_image_type()

        src_ref = f"{rayci_work_repo}:{ctx.wanda_tag}"
        logger.info(f"Verifying source image in Wanda cache: {src_ref}")
        if not _image_exists(src_ref):
            raise PushRayImageError(f"Source image not found in Wanda cache: {src_ref}")

        # Re-emit the per-image pip freeze as a Buildkite artifact (cpu only).
        # Downstream consumer: anyscale/product
        # devprod/rayrelease/releaser.py:update_doc_with_latest_docker_dependencies.
        # Runs regardless of dry_run: the shipped-deps record is independent of
        # whether we push to Docker Hub.
        #
        # Best-effort by design: this publish step is skip-on-premerge, so the
        # path only ever runs in postmerge and cannot be validated pre-merge.
        # A failure here must never break image publishing (the critical path),
        # so swallow and log any error rather than propagating it.
        if plat == "cpu":
            try:
                _export_pip_freeze(src_ref, ctx)
            except Exception as e:
                logger.error(
                    f"Failed to stage pip-freeze artifact for {src_ref}; "
                    f"continuing without it: {e}",
                    exc_info=True,
                )

        destination_tags = ctx.destination_tags()
        for tag in destination_tags:
            dest_ref = f"{ctx.docker_hub_repo}:{tag}"
            _copy_image(src_ref, dest_ref, dry_run=dry_run)

        all_tags.extend(destination_tags)
        logger.info(f"Completed platform {plat} with tags: {destination_tags}")

    logger.info(
        f"\nSuccessfully processed {len(platforms)} platform(s) for {image_type}"
    )
    logger.info(f"Total tags: {len(all_tags)}")


if __name__ == "__main__":
    main()
