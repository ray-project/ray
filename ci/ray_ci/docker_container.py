import os

from ci.ray_ci.container import Container, _DOCKER_ECR_REPO
from ci.ray_ci.builder_container import PYTHON_VERSIONS
from ci.ray_ci.utils import docker_pull, RAY_VERSION


PLATFORM = ["cu118"]


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

        version_tag = os.environ["BUILDKITE_COMMIT"][:6]
        project = f"rayproject/{self.image_type}"
        ray_image = f"{project}:{version_tag}-{self.python_version}-{self.platform}"

        self.run_script(
            [
                "./ci/build/build-ray-docker.sh "
                f"{wheel_name} {base_image} {constraints_file} {ray_image}"
            ]
        )
