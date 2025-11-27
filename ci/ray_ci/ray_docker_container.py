import os
from typing import List, Optional

from ci.ray_ci.configs import DEFAULT_ARCHITECTURE, PYTHON_VERSIONS
from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.docker_container import RAY_REPO_MAP, DockerContainer, RayType
from ci.ray_ci.utils import RAY_VERSION, docker_pull

from ray_release.configs.global_config import get_global_config


class RayDockerContainer(DockerContainer):
    """
    Container for building and publishing ray docker images
    """

    def run(self, base: Optional[str] = None) -> None:
        """
        Build and publish ray docker images
        """
        assert "RAYCI_BUILD_ID" in os.environ, "RAYCI_BUILD_ID not set"
        rayci_build_id = os.environ["RAYCI_BUILD_ID"]
        if base is None:
            if self.image_type in [
                RayType.RAY_EXTRA.value,
                RayType.RAY_ML_EXTRA.value,
                RayType.RAY_LLM_EXTRA.value,
            ]:
                base = "base-extra"
            else:
                base = "base"

        if self.architecture == DEFAULT_ARCHITECTURE:
            suffix = base
        else:
            suffix = f"{base}-{self.architecture}"

        image_repo = RAY_REPO_MAP[self.image_type]

        base_image = (
            f"{_DOCKER_ECR_REPO}:{rayci_build_id}"
            f"-{image_repo}-py{self.python_version}-{self.platform}-{suffix}"
        )

        docker_pull(base_image)

        bin_path = PYTHON_VERSIONS[self.python_version]["bin_path"]
        wheel_name = (
            f"ray-{RAY_VERSION}-{bin_path}-manylinux2014_{self.architecture}.whl"
        )
        constraints_file = "requirements_compiled.txt"
        tag = self._get_canonical_tag()
        ray_image = f"rayproject/{image_repo}:{tag}"
        pip_freeze = f"{self.image_type}:{tag}_pip-freeze.txt"

        cmds = [
            "./ci/build/build-ray-docker.sh "
            f"{wheel_name} {base_image} {constraints_file} {ray_image} {pip_freeze}"
        ]
        if self._should_upload():
            cmds += [
                "bazel run .buildkite:copy_files -- --destination docker_login",
            ]
            for alias in self._get_image_names():
                cmds += [
                    f"docker tag {ray_image} {alias}",
                    f"docker push {alias}",
                ]
        self.run_script(cmds)

    def _should_upload(self) -> bool:
        if not self.upload:
            return False
        if (
            os.environ.get("BUILDKITE_PIPELINE_ID")
            not in get_global_config()["ci_pipeline_postmerge"]
        ):
            return False
        if os.environ.get("BUILDKITE_BRANCH", "").startswith("releases/"):
            return True
        return (
            os.environ.get("BUILDKITE_BRANCH") == "master"
            and os.environ.get("RAYCI_SCHEDULE") == "nightly"
        )

    def _get_image_names(self) -> List[str]:
        repo_name = RAY_REPO_MAP[self.image_type]
        ray_repo = f"rayproject/{repo_name}"

        return [f"{ray_repo}:{tag}" for tag in self._get_image_tags(external=True)]
