import os
from typing import List

from ci.ray_ci.container import Container, _DOCKER_ECR_REPO
from ci.ray_ci.builder_container import PYTHON_VERSIONS
from ci.ray_ci.utils import docker_pull, RAY_VERSION, POSTMERGE_PIPELINE


PLATFORM = ["cu118"]
GPU_PLATFORM = "cu118"


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

    def run(self) -> None:
        """
        Build and publish ray docker images
        """
        assert "RAYCI_BUILD_ID" in os.environ, "RAYCI_BUILD_ID not set"
        rayci_build_id = os.environ["RAYCI_BUILD_ID"]

        base_image = (
            f"{_DOCKER_ECR_REPO}:{rayci_build_id}"
            f"-{self.image_type}{self.python_version}{self.platform}base"
        )
        docker_pull(base_image)

        bin_path = PYTHON_VERSIONS[self.python_version]["bin_path"]
        wheel_name = f"ray-{RAY_VERSION}-{bin_path}-manylinux2014_x86_64.whl"

        constraints_file = "requirements_compiled.txt"
        if self.python_version == "py37":
            constraints_file = "requirements_compiled_py37.txt"

        ray_images = self._get_image_names()
        ray_image = ray_images[0]
        cmds = [
            "./ci/build/build-ray-docker.sh "
            f"{wheel_name} {base_image} {constraints_file} {ray_image}"
        ]
        if self._should_upload():
            cmds += [
                "pip install -q aws_requests_auth boto3",
                "python .buildkite/copy_files.py --destination docker_login",
            ]
            for alias in self._get_image_names():
                cmds += [
                    f"docker tag {ray_image} {alias}",
                    f"docker push {alias}",
                ]
        self.run_script(cmds)

    def _should_upload(self) -> bool:
        return os.environ.get("BUILDKITE_PIPELINE_ID") == POSTMERGE_PIPELINE

    def _get_image_names(self) -> List[str]:
        # Image name is composed by ray version tag, python version and platform.
        # See https://docs.ray.io/en/latest/ray-overview/installation.html for
        # more information on the image tags.
        versions = [f"{os.environ['BUILDKITE_COMMIT'][:6]}"]
        if os.environ.get("BUILDKITE_BRANCH") == "master":
            # TODO(can): add ray version if this is a release branch
            versions.append("nightly")

        platforms = [f"-{self.platform}"]
        if self.platform == "cpu" and self.image_type == "ray":
            # no tag is alias to cpu for ray image
            platforms.append("")
        elif self.platform == GPU_PLATFORM:
            # gpu is alias to cu118 for ray image
            platforms.append("-gpu")
            if self.image_type == "ray-ml":
                # no tag is alias to gpu for ray-ml image
                platforms.append("")

        alias_images = []
        ray_repo = f"rayproject/{self.image_type}"
        for version in versions:
            for platform in platforms:
                alias = f"{ray_repo}:{version}-{self.python_version}{platform}"
                alias_images.append(alias)

        return alias_images
