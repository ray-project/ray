import os
from typing import List

from ci.ray_ci.container import Container


PLATFORM = [
    "cpu",
    "cu11.5.2",
    "cu11.6.2",
    "cu11.7.1",
    "cu11.8.0",
    "cu12.1.1",
]
GPU_PLATFORM = "cu11.8.0"
DEFAULT_PYTHON_VERSION = "3.8"


class DockerContainer(Container):
    """
    Container for building and publishing ray docker images
    """

    def __init__(self, python_version: str, platform: str, image_type: str) -> None:
        assert "RAYCI_CHECKOUT_DIR" in os.environ, "RAYCI_CHECKOUT_DIR not set"
        rayci_checkout_dir = os.environ["RAYCI_CHECKOUT_DIR"]
        self.python_version = python_version
        self.platform = platform
        self.image_type = image_type

        super().__init__(
            "forge",
            volumes=[
                f"{rayci_checkout_dir}:/rayci",
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
        )

    def _get_image_version_tags(self) -> List[str]:
        branch = os.environ.get("BUILDKITE_BRANCH")
        sha_tag = os.environ["BUILDKITE_COMMIT"][:6]
        if branch == "master":
            return [sha_tag, "nightly"]

        if branch and branch.startswith("releases/"):
            release_name = branch[len("releases/") :]
            return [f"{release_name}.{sha_tag}"]

        return [sha_tag]

    def _get_canonical_tag(self) -> str:
        # The canonical tag is the first tag in the list of tags. The list of tag is
        # never empty because the image is always tagged with at least the sha tag.
        #
        # The canonical tag is the most complete tag with no abbreviation,
        # e.g. sha-pyversion-platform
        return self._get_image_tags()[0]

    def get_python_version_tag(self) -> str:
        return f"-py{self.python_version.replace('.', '')}"  # 3.8 -> py38

    def get_platform_tag(self) -> str:
        if self.platform == "cpu":
            return "-cpu"
        versions = self.platform.split(".")
        return f"-{versions[0]}{versions[1]}"  # cu11.8.0 -> cu118

    def _get_image_tags(self) -> List[str]:
        # An image tag is composed by ray version tag, python version and platform.
        # See https://docs.ray.io/en/latest/ray-overview/installation.html for
        # more information on the image tags.
        versions = self._get_image_version_tags()

        platforms = [self.get_platform_tag()]
        if self.platform == "cpu" and self.image_type == "ray":
            # no tag is alias to cpu for ray image
            platforms.append("")
        elif self.platform == GPU_PLATFORM:
            # gpu is alias to cu118 for ray image
            platforms.append("-gpu")
            if self.image_type == "ray-ml":
                # no tag is alias to gpu for ray-ml image
                platforms.append("")

        py_versions = [self.get_python_version_tag()]
        if self.python_version == DEFAULT_PYTHON_VERSION:
            py_versions.append("")

        tags = []
        for version in versions:
            for platform in platforms:
                for py_version in py_versions:
                    tag = f"{version}{py_version}{platform}"
                    tags.append(tag)
        return tags
