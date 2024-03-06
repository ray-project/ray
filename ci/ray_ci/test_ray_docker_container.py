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
        ), mock.patch.dict(
            os.environ, {"BUILDKITE_COMMIT": "abc123def456"}
        ):
            formatted_date = datetime.now().strftime("%y%m%d")
            sha = os.environ["BUILDKITE_COMMIT"][:6]
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
                f"rayproject/ray:nightly.{formatted_date}.{sha}-{pv}-cu118 "
                f"ray:nightly.{formatted_date}.{sha}-{pv}-cu118_pip-freeze.txt"
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
                f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}-cpu "
                f"ray-ml:nightly.{formatted_date}.{sha}-{pv}-cpu_pip-freeze.txt"
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
            os.environ, {"RAYCI_SCHEDULE": "nightly"}
        ), mock.patch.dict(
            os.environ, {"BUILDKITE_COMMIT": "abc123def456"}
        ):
            formatted_date = datetime.now().strftime("%y%m%d")
            sha = os.environ["BUILDKITE_COMMIT"][:6]
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)

            container = RayDockerContainer(v, "cu11.8.0", "ray")
            container.run()
            assert len(self.cmds) == 19
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-py{v}-cu11.8.0-base "
                "requirements_compiled.txt "
                f"rayproject/ray:nightly.{formatted_date}.{sha}-{pv}-cu118 "
                f"ray:nightly.{formatted_date}.{sha}-{pv}-cu118_pip-freeze.txt"
            )
            assert self.cmds[1] == "pip install -q aws_requests_auth boto3"
            assert (
                self.cmds[2]
                == "python .buildkite/copy_files.py --destination docker_login"
            )
            for i in range(3, 11):  # check nightly alias
                assert f"nightly.{formatted_date}.{sha}" in self.cmds[i]
            for i in range(11, len(self.cmds)):  # check nightly date alias
                assert "nightly-" in self.cmds[i]

            # Run with ray-ml
            self.cmds = []
            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray-ml")
            container.run()
            assert len(self.cmds) == 7
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}-cpu "
                f"ray-ml:nightly.{formatted_date}.{sha}-{pv}-cpu_pip-freeze.txt"
            )
            assert self.cmds[1] == "pip install -q aws_requests_auth boto3"
            assert (
                self.cmds[2]
                == "python .buildkite/copy_files.py --destination docker_login"
            )
            for i in range(3, 5):  # check nightly alias
                assert f"nightly.{formatted_date}.{sha}" in self.cmds[i]
            for i in range(5, len(self.cmds)):  # check nightly date alias
                assert "nightly-" in self.cmds[i]

    def test_run_daytime(self) -> None:
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
            os.environ, {"RAYCI_SCHEDULE": "daytime"}
        ), mock.patch.dict(
            os.environ, {"BUILDKITE_COMMIT": "abc123def456"}
        ):
            formatted_date = datetime.now().strftime("%y%m%d")
            sha = os.environ["BUILDKITE_COMMIT"][:6]
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cu11.8.0", "ray")
            container.run()

            assert len(self.cmds) == 3
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-py{v}-cu11.8.0-base "
                "requirements_compiled.txt "
                f"rayproject/ray:nightly.{formatted_date}.{sha}-{pv}-cu118 "
                f"ray:nightly.{formatted_date}.{sha}-{pv}-cu118_pip-freeze.txt"
            )
            assert self.cmds[1] == "pip install -q aws_requests_auth boto3"
            assert (
                self.cmds[2]
                == "python .buildkite/copy_files.py --destination docker_login"
            )

            # Run with ray-ml
            self.cmds = []
            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray-ml")
            container.run()
            assert len(self.cmds) == 3
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:123-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}-cpu "
                f"ray-ml:nightly.{formatted_date}.{sha}-{pv}-cpu_pip-freeze.txt"
            )
            assert self.cmds[1] == "pip install -q aws_requests_auth boto3"
            assert (
                self.cmds[2]
                == "python .buildkite/copy_files.py --destination docker_login"
            )

    def test_canonical_tag(self) -> None:
        formatted_date = datetime.now().strftime("%y%m%d")
        sha = os.environ["BUILDKITE_COMMIT"][:6]
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray", canonical_tag="abc")
        assert container._get_canonical_tag() == "abc"

        container = RayDockerContainer(v, "cpu", "ray")
        assert (
            container._get_canonical_tag() == f"nightly.{formatted_date}.{sha}-{pv}-cpu"
        )

        container = RayDockerContainer(v, "cpu", "ray", "aarch64")
        assert (
            container._get_canonical_tag()
            == f"nightly.{formatted_date}.{sha}-{pv}-cpu-aarch64"
        )

        container = RayDockerContainer(v, "cu11.8.0", "ray-ml")
        assert (
            container._get_canonical_tag()
            == f"nightly.{formatted_date}.{sha}-{pv}-cu118"
        )

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
        sha = os.environ["BUILDKITE_COMMIT"][:6]
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray")
        formatted_date = datetime.now().strftime("%y%m%d")
        assert container._get_image_tags() == [
            f"nightly.{formatted_date}.{sha}-{pv}-cpu",
            f"nightly.{formatted_date}.{sha}-cpu",
            f"nightly.{formatted_date}.{sha}-{pv}",
            f"nightly.{formatted_date}.{sha}",
            f"nightly-{pv}-cpu",
            "nightly-cpu",
            f"nightly-{pv}",
            "nightly",
        ]

    def test_get_image_name(self) -> None:
        sha = os.environ["BUILDKITE_COMMIT"][:6]
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        formatted_date = datetime.now().strftime("%y%m%d")
        container = RayDockerContainer(v, "cpu", "ray")
        assert container._get_image_names() == [
            f"rayproject/ray:nightly.{formatted_date}.{sha}-{pv}-cpu",
            f"rayproject/ray:nightly.{formatted_date}.{sha}-cpu",
            f"rayproject/ray:nightly.{formatted_date}.{sha}-{pv}",
            f"rayproject/ray:nightly.{formatted_date}.{sha}",
            f"rayproject/ray:nightly-{pv}-cpu",
            "rayproject/ray:nightly-cpu",
            f"rayproject/ray:nightly-{pv}",
            "rayproject/ray:nightly",
        ]

        v = self.get_non_default_python()
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cu11.8.0", "ray-ml")
        assert container._get_image_names() == [
            f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}-cu118",
            f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}-gpu",
            f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}",
            f"rayproject/ray-ml:nightly-{pv}-cu118",
            f"rayproject/ray-ml:nightly-{pv}-gpu",
            f"rayproject/ray-ml:nightly-{pv}",
        ]

        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "releases/1.0.0"}):
            v = DEFAULT_PYTHON_VERSION
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray")
            assert container._get_image_names() == [
                f"rayproject/ray:1.0.0.123456-{pv}-cpu",
                "rayproject/ray:1.0.0.123456-cpu",
                f"rayproject/ray:1.0.0.123456-{pv}",
                "rayproject/ray:1.0.0.123456",
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
