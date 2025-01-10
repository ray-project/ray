import os
from typing import List
from datetime import datetime
from enum import Enum

from ci.ray_ci.linux_container import LinuxContainer
from ci.ray_ci.builder_container import DEFAULT_ARCHITECTURE, DEFAULT_PYTHON_VERSION


PLATFORMS_RAY = [
    "cpu",
    "cu11.7.1-cudnn8",
    "cu11.8.0-cudnn8",
    "cu12.1.1-cudnn8",
    "cu12.3.2-cudnn9",
]
PLATFORMS_RAY_ML = [
    "cpu",
    "cu12.1.1-cudnn8",
]
GPU_PLATFORM = "cu12.1.1-cudnn8"

PYTHON_VERSIONS_RAY = ["3.9", "3.10", "3.11", "3.12"]
PYTHON_VERSIONS_RAY_ML = ["3.9", "3.10", "3.11"]
ARCHITECTURES_RAY = ["x86_64", "aarch64"]
ARCHITECTURES_RAY_ML = ["x86_64"]


class RayType(str, Enum):
    RAY = "ray"
    RAY_ML = "ray-ml"


class DockerContainer(LinuxContainer):
    """
    Container for building and publishing ray docker images
    """

    def __init__(
        self,
        python_version: str,
        platform: str,
        image_type: str,
        architecture: str = DEFAULT_ARCHITECTURE,
        canonical_tag: str = None,
        upload: bool = False,
    ) -> None:
        assert "RAYCI_CHECKOUT_DIR" in os.environ, "RAYCI_CHECKOUT_DIR not set"

        assert python_version in PYTHON_VERSIONS_RAY
        assert platform in PLATFORMS_RAY
        assert architecture in ARCHITECTURES_RAY
        if image_type == RayType.RAY_ML:
            assert python_version in PYTHON_VERSIONS_RAY_ML
            assert platform in PLATFORMS_RAY_ML
            assert architecture in ARCHITECTURES_RAY_ML

        rayci_checkout_dir = os.environ["RAYCI_CHECKOUT_DIR"]
        self.python_version = python_version
        self.platform = platform
        self.image_type = image_type
        self.architecture = architecture
        self.canonical_tag = canonical_tag
        self.upload = upload

        super().__init__(
            "forge" if architecture == "x86_64" else "forge-aarch64",
            volumes=[
                f"{rayci_checkout_dir}:/rayci",
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
        )

    def _get_image_version_tags(self, external: bool) -> List[str]:
        """
        Get version tags.

        Args:
            external: If True, return the external image tags. If False, return the
                internal image tags.
        """
        branch = os.environ.get("BUILDKITE_BRANCH")
        sha_tag = os.environ["BUILDKITE_COMMIT"][:6]
        pr = os.environ.get("BUILDKITE_PULL_REQUEST", "false")
        formatted_date = datetime.now().strftime("%y%m%d")

        if branch == "master":
            if external and os.environ.get("RAYCI_SCHEDULE") == "nightly":
                return [f"nightly.{formatted_date}.{sha_tag}", "nightly"]
            return [sha_tag]

        if branch and branch.startswith("releases/"):
            release_name = branch[len("releases/") :]
            return [f"{release_name}.{sha_tag}"]

        if pr != "false":
            return [f"pr-{pr}.{sha_tag}"]

        return [sha_tag]

    def _get_canonical_tag(self) -> str:
        # The canonical tag is the first tag in the list of tags. The list of tag is
        # never empty because the image is always tagged with at least the sha tag.
        #
        # The canonical tag is the most complete tag with no abbreviation,
        # e.g. sha-pyversion-platform
        return self.canonical_tag if self.canonical_tag else self._get_image_tags()[0]

    def get_python_version_tag(self) -> str:
        return f"-py{self.python_version.replace('.', '')}"  # 3.x -> py3x

    def get_platform_tag(self) -> str:
        if self.platform == "cpu":
            return "-cpu"
        versions = self.platform.split(".")
        return f"-{versions[0]}{versions[1]}"  # cu11.8.0 -> cu118

    def _get_image_tags(self, external: bool = False) -> List[str]:
        """
        Get image tags & alias for the container image.

        An image tag is composed by ray version tag, python version and platform.
        See https://docs.ray.io/en/latest/ray-overview/installation.html for
        more information on the image tags.

        Args:
            external: If True, return the external image tags. If False, return the
                internal image tags.
        """

        versions = self._get_image_version_tags(external)

        platforms = [self.get_platform_tag()]
        if self.platform == "cpu" and self.image_type == RayType.RAY:
            # no tag is alias to cpu for ray image
            platforms.append("")
        elif self.platform == GPU_PLATFORM:
            # gpu is alias to cu118 for ray image
            platforms.append("-gpu")
            if self.image_type == RayType.RAY_ML:
                # no tag is alias to gpu for ray-ml image
                platforms.append("")

        py_versions = [self.get_python_version_tag()]
        if self.python_version == DEFAULT_PYTHON_VERSION:
            py_versions.append("")

        tags = []
        for version in versions:
            for platform in platforms:
                for py_version in py_versions:
                    if self.architecture == DEFAULT_ARCHITECTURE:
                        tag = f"{version}{py_version}{platform}"
                    else:
                        tag = f"{version}{py_version}{platform}-{self.architecture}"
                    tags.append(tag)
        return tags
