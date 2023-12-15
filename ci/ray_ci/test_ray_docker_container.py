import os
import sys
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.ray_docker_container import RayDockerContainer
from ci.ray_ci.test_base import RayCITestBase
from ci.ray_ci.utils import RAY_VERSION


class TestRayDockerContainer(RayCITestBase):
    cmds = []

    def test_run(self) -> None:
        def _mock_run_script(input: List[str]) -> None:
            self.cmds.append(input[0])

        with mock.patch(
            "ci.ray_ci.ray_docker_container.docker_pull", return_value=None
        ), mock.patch(
            "ci.ray_ci.docker_container.LinuxContainer.run_script",
            side_effect=_mock_run_script,
        ):
            container = RayDockerContainer("3.8", "cu11.8.0", "ray")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-cp38-cp38-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-py3.8-cu11.8.0-base "
                "requirements_compiled.txt "
                "rayproject/ray:123456-py38-cu118 "
                "ray:123456-py38-cu118_pip-freeze.txt"
            )

            container = RayDockerContainer("3.9", "cpu", "ray-ml")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-cp39-cp39-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-ml-py3.9-cpu-base "
                "requirements_compiled.txt "
                "rayproject/ray-ml:123456-py39-cpu "
                "ray-ml:123456-py39-cpu_pip-freeze.txt"
            )

    def test_canonical_tag(self) -> None:
        container = RayDockerContainer("3.8", "cpu", "ray", canonical_tag="abc")
        assert container._get_canonical_tag() == "abc"

        container = RayDockerContainer("3.8", "cpu", "ray")
        assert container._get_canonical_tag() == "123456-py38-cpu"

        container = RayDockerContainer("3.8", "cpu", "ray", "aarch64")
        assert container._get_canonical_tag() == "123456-py38-cpu-aarch64"

        container = RayDockerContainer("3.8", "cu11.8.0", "ray-ml")
        assert container._get_canonical_tag() == "123456-py38-cu118"

        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "releases/1.0.0"}):
            container = RayDockerContainer("3.8", "cpu", "ray")
            assert container._get_canonical_tag() == "1.0.0.123456-py38-cpu"

        with mock.patch.dict(
            os.environ, {"BUILDKITE_BRANCH": "abc", "BUILDKITE_PULL_REQUEST": "123"}
        ):
            container = RayDockerContainer("3.8", "cpu", "ray")
            assert container._get_canonical_tag() == "pr-123.123456-py38-cpu"

    def test_get_image_tags(self) -> None:
        # bulk logic of _get_image_tags is tested in its callers (get_image_name and
        # get_canonical_tag), so we only test the basic cases here
        container = RayDockerContainer("3.8", "cpu", "ray")
        assert container._get_image_tags() == [
            "123456-py38-cpu",
            "123456-cpu",
            "123456-py38",
            "123456",
            "nightly-py38-cpu",
            "nightly-cpu",
            "nightly-py38",
            "nightly",
        ]

    def test_get_image_name(self) -> None:
        container = RayDockerContainer("3.8", "cpu", "ray")
        assert container._get_image_names() == [
            "rayproject/ray:123456-py38-cpu",
            "rayproject/ray:123456-cpu",
            "rayproject/ray:123456-py38",
            "rayproject/ray:123456",
            "rayproject/ray:nightly-py38-cpu",
            "rayproject/ray:nightly-cpu",
            "rayproject/ray:nightly-py38",
            "rayproject/ray:nightly",
        ]

        container = RayDockerContainer("3.9", "cu11.8.0", "ray-ml")
        assert container._get_image_names() == [
            "rayproject/ray-ml:123456-py39-cu118",
            "rayproject/ray-ml:123456-py39-gpu",
            "rayproject/ray-ml:123456-py39",
            "rayproject/ray-ml:nightly-py39-cu118",
            "rayproject/ray-ml:nightly-py39-gpu",
            "rayproject/ray-ml:nightly-py39",
        ]

        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "releases/1.0.0"}):
            container = RayDockerContainer("3.8", "cpu", "ray")
            assert container._get_image_names() == [
                "rayproject/ray:1.0.0.123456-py38-cpu",
                "rayproject/ray:1.0.0.123456-cpu",
                "rayproject/ray:1.0.0.123456-py38",
                "rayproject/ray:1.0.0.123456",
            ]

    def test_get_python_version_tag(self) -> None:
        container = RayDockerContainer("3.8", "cpu", "ray")
        assert container.get_python_version_tag() == "-py38"

    def test_get_platform_tag(self) -> None:
        container = RayDockerContainer("3.8", "cpu", "ray")
        assert container.get_platform_tag() == "-cpu"

        container = RayDockerContainer("3.8", "cu11.8.0", "ray")
        assert container.get_platform_tag() == "-cu118"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
