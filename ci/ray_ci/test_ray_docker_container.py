import os
import sys
from typing import List
from unittest import mock
from datetime import datetime
import pytest

from ci.ray_ci.builder_container import DEFAULT_PYTHON_VERSION
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
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cu11.8.0", "ray")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-py{v}-cu11.8.0-base "
                "requirements_compiled.txt "
                f"rayproject/ray:123456-{pv}-cu118 "
                f"ray:123456-{pv}-cu118_pip-freeze.txt"
            )

            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray-ml")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:123456-{pv}-cpu "
                f"ray-ml:123456-{pv}-cpu_pip-freeze.txt"
            )

    def test_run_nightly(self) -> None:
        def _mock_run_script(input: List[str]) -> None:
            for i in input:
                self.cmds.append(i)

        with mock.patch(
            "ci.ray_ci.ray_docker_container.docker_pull", return_value=None
        ), mock.patch(
            "ci.ray_ci.docker_container.LinuxContainer.run_script",
            side_effect=_mock_run_script,
        ), mock.patch(
            "ci.ray_ci.ray_docker_container.RayDockerContainer._should_upload",
            return_value=True,
        ), mock.patch.dict(
            os.environ, {"RAYCI_SCHEDULE": "NIGHTLY"}
        ):
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            formatted_date = datetime.now().strftime("%y%m%d")
            container = RayDockerContainer(v, "cu11.8.0", "ray")
            container.run()
            cmd = self.cmds[0]
            first_run_cmd_count = len(self.cmds)
            assert first_run_cmd_count == 19

            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-py{v}-cu11.8.0-base "
                "requirements_compiled.txt "
                f"rayproject/ray:123456-{pv}-cu118 "
                f"ray:123456-{pv}-cu118_pip-freeze.txt"
            )
            # Start from index 11 since first 10 commands
            # are for building image and push commit tags
            # index 0: build docker image
            # index 1: pip install -q aws_requests_auth boto3
            # index 2: python .buildkite/copy_files.py --destination docker_login
            for i in range(11, first_run_cmd_count):
                assert f"nightly.{formatted_date}" in self.cmds[i]

            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray-ml")
            container.run()
            cmd = self.cmds[first_run_cmd_count]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:123456-{pv}-cpu "
                f"ray-ml:123456-{pv}-cpu_pip-freeze.txt"
            )
            for i in range(first_run_cmd_count + 5, len(self.cmds)):
                assert f"nightly.{formatted_date}" in self.cmds[i]

    def test_canonical_tag(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray", canonical_tag="abc")
        assert container._get_canonical_tag() == "abc"

        container = RayDockerContainer(v, "cpu", "ray")
        assert container._get_canonical_tag() == f"123456-{pv}-cpu"

        container = RayDockerContainer(v, "cpu", "ray", "aarch64")
        assert container._get_canonical_tag() == f"123456-{pv}-cpu-aarch64"

        container = RayDockerContainer(v, "cu11.8.0", "ray-ml")
        assert container._get_canonical_tag() == f"123456-{pv}-cu118"

        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "releases/1.0.0"}):
            container = RayDockerContainer(v, "cpu", "ray")
            assert container._get_canonical_tag() == f"1.0.0.123456-{pv}-cpu"

        with mock.patch.dict(
            os.environ, {"BUILDKITE_BRANCH": "abc", "BUILDKITE_PULL_REQUEST": "123"}
        ):
            container = RayDockerContainer(v, "cpu", "ray")
            assert container._get_canonical_tag() == f"pr-123.123456-{pv}-cpu"

    def test_get_image_tags(self) -> None:
        # bulk logic of _get_image_tags is tested in its callers (get_image_name and
        # get_canonical_tag), so we only test the basic cases here
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray")
        formatted_date = datetime.now().strftime("%y%m%d")
        assert container._get_image_tags() == [
            f"123456-{pv}-cpu",
            "123456-cpu",
            f"123456-{pv}",
            "123456",
            f"nightly.{formatted_date}-{pv}-cpu",
            f"nightly.{formatted_date}-cpu",
            f"nightly.{formatted_date}-{pv}",
            f"nightly.{formatted_date}",
        ]

    def test_get_image_name(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        formatted_date = datetime.now().strftime("%y%m%d")
        container = RayDockerContainer(v, "cpu", "ray")
        assert container._get_image_names() == [
            f"rayproject/ray:123456-{pv}-cpu",
            "rayproject/ray:123456-cpu",
            f"rayproject/ray:123456-{pv}",
            "rayproject/ray:123456",
            f"rayproject/ray:nightly.{formatted_date}-{pv}-cpu",
            f"rayproject/ray:nightly.{formatted_date}-cpu",
            f"rayproject/ray:nightly.{formatted_date}-{pv}",
            f"rayproject/ray:nightly.{formatted_date}",
        ]

        v = self.get_non_default_python()
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cu11.8.0", "ray-ml")
        assert container._get_image_names() == [
            f"rayproject/ray-ml:123456-{pv}-cu118",
            f"rayproject/ray-ml:123456-{pv}-gpu",
            f"rayproject/ray-ml:123456-{pv}",
            f"rayproject/ray-ml:nightly.{formatted_date}-{pv}-cu118",
            f"rayproject/ray-ml:nightly.{formatted_date}-{pv}-gpu",
            f"rayproject/ray-ml:nightly.{formatted_date}-{pv}",
        ]

    def test_get_python_version_tag(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray")
        assert container.get_python_version_tag() == f"-{pv}"

    def test_get_platform_tag(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        container = RayDockerContainer(v, "cpu", "ray")
        assert container.get_platform_tag() == "-cpu"

        container = RayDockerContainer(v, "cu11.8.0", "ray")
        assert container.get_platform_tag() == "-cu118"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
