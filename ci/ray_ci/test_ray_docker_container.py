import os
import sys
from typing import List
from unittest import mock
from datetime import datetime
import pytest

from ci.ray_ci.builder_container import DEFAULT_PYTHON_VERSION
from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.docker_container import GPU_PLATFORM
from ci.ray_ci.ray_docker_container import RayDockerContainer
from ci.ray_ci.test_base import RayCITestBase
from ci.ray_ci.utils import RAY_VERSION
from ray_release.configs.global_config import get_global_config


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
            sha = "123456"
            ray_ci_build_id = "123"
            cuda = "cu11.8.0-cudnn8"

            # Run with default python version and ray image
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, cuda, "ray")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-py{v}-{cuda}-base "
                "requirements_compiled.txt "
                f"rayproject/ray:{sha}-{pv}-cu118 "
                f"ray:{sha}-{pv}-cu118_pip-freeze.txt"
            )

            # Run with non-default python version and ray-ml image
            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray-ml")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:{sha}-{pv}-cpu "
                f"ray-ml:{sha}-{pv}-cpu_pip-freeze.txt"
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
        ):
            formatted_date = datetime.now().strftime("%y%m%d")
            sha = "123456"
            ray_ci_build_id = "123"

            # Run with default python version and ray image
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            cuda = "cu12.1.1-cudnn8"
            container = RayDockerContainer(v, cuda, "ray")
            container.run()
            assert len(self.cmds) == 19
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-py{v}-{cuda}-base "
                "requirements_compiled.txt "
                f"rayproject/ray:{sha}-{pv}-cu121 "
                f"ray:{sha}-{pv}-cu121_pip-freeze.txt"
            )
            assert self.cmds[1] == "pip install -q aws_requests_auth boto3"
            assert (
                self.cmds[2]
                == "python .buildkite/copy_files.py --destination docker_login"
            )
            for i in range(3, 11):  # check nightly.date.sha alias
                assert f"nightly.{formatted_date}.{sha}" in self.cmds[i]
            for i in range(11, len(self.cmds)):  # check nightly alias
                assert "nightly-" in self.cmds[i]

            # Run with non-default python version and ray-ml image
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
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:{sha}-{pv}-cpu "
                f"ray-ml:{sha}-{pv}-cpu_pip-freeze.txt"
            )
            assert self.cmds[1] == "pip install -q aws_requests_auth boto3"
            assert (
                self.cmds[2]
                == "python .buildkite/copy_files.py --destination docker_login"
            )
            for i in range(3, 5):  # check nightly.date.sha alias
                assert f"nightly.{formatted_date}.{sha}" in self.cmds[i]
            for i in range(5, len(self.cmds)):  # check nightly alias
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
            return_value=False,
        ), mock.patch.dict(
            os.environ, {"RAYCI_SCHEDULE": "daytime"}
        ):
            sha = "123456"
            ray_ci_build_id = "123"
            cuda = "cu11.8.0-cudnn8"

            # Run with default python version and ray image
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, cuda, "ray")
            container.run()
            assert len(self.cmds) == 1
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-py{v}-{cuda}-base "
                "requirements_compiled.txt "
                f"rayproject/ray:{sha}-{pv}-cu118 "
                f"ray:{sha}-{pv}-cu118_pip-freeze.txt"
            )

            # Run with non-default python version and ray-ml image
            self.cmds = []
            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray-ml")
            container.run()
            assert len(self.cmds) == 1
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:{sha}-{pv}-cpu "
                f"ray-ml:{sha}-{pv}-cpu_pip-freeze.txt"
            )

    def test_canonical_tag(self) -> None:
        sha = "123456"
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray", canonical_tag="abc")
        assert container._get_canonical_tag() == "abc"

        container = RayDockerContainer(v, "cpu", "ray")
        assert container._get_canonical_tag() == f"{sha}-{pv}-cpu"

        container = RayDockerContainer(v, "cpu", "ray", "aarch64")
        assert container._get_canonical_tag() == f"{sha}-{pv}-cpu-aarch64"

        container = RayDockerContainer(v, GPU_PLATFORM, "ray-ml")
        assert container._get_canonical_tag() == f"{sha}-{pv}-cu121"

        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "releases/1.0.0"}):
            container = RayDockerContainer(v, "cpu", "ray")
            assert container._get_canonical_tag() == f"1.0.0.{sha}-{pv}-cpu"

        with mock.patch.dict(
            os.environ, {"BUILDKITE_BRANCH": "abc", "BUILDKITE_PULL_REQUEST": "123"}
        ):
            container = RayDockerContainer(v, "cpu", "ray")
            assert container._get_canonical_tag() == f"pr-123.{sha}-{pv}-cpu"

    def test_get_image_tags(self) -> None:
        # bulk logic of _get_image_tags is tested in its callers (get_image_name and
        # get_canonical_tag), so we only test the basic cases here
        sha = "123456"
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray")
        formatted_date = datetime.now().strftime("%y%m%d")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_tags(external=True) == [
                f"{sha}-{pv}-cpu",
                f"{sha}-cpu",
                f"{sha}-{pv}",
                f"{sha}",
            ]
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "nightly"}):
            assert container._get_image_tags(external=True) == [
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
        sha = "123456"
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        formatted_date = datetime.now().strftime("%y%m%d")
        container = RayDockerContainer(v, "cpu", "ray")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_names() == [
                f"rayproject/ray:{sha}-{pv}-cpu",
                f"rayproject/ray:{sha}-cpu",
                f"rayproject/ray:{sha}-{pv}",
                f"rayproject/ray:{sha}",
            ]

        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "nightly"}):
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
        container = RayDockerContainer(v, "cu12.1.1-cudnn8", "ray-ml")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_names() == [
                f"rayproject/ray-ml:{sha}-{pv}-cu121",
                f"rayproject/ray-ml:{sha}-{pv}-gpu",
                f"rayproject/ray-ml:{sha}-{pv}",
            ]

        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "nightly"}):
            assert container._get_image_names() == [
                f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}-cu121",
                f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}-gpu",
                f"rayproject/ray-ml:nightly.{formatted_date}.{sha}-{pv}",
                f"rayproject/ray-ml:nightly-{pv}-cu121",
                f"rayproject/ray-ml:nightly-{pv}-gpu",
                f"rayproject/ray-ml:nightly-{pv}",
            ]

        release_version = "1.0.0"
        with mock.patch.dict(
            os.environ, {"BUILDKITE_BRANCH": f"releases/{release_version}"}
        ):
            v = DEFAULT_PYTHON_VERSION
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray")
            assert container._get_image_names() == [
                f"rayproject/ray:{release_version}.{sha}-{pv}-cpu",
                f"rayproject/ray:{release_version}.{sha}-cpu",
                f"rayproject/ray:{release_version}.{sha}-{pv}",
                f"rayproject/ray:{release_version}.{sha}",
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

        container = RayDockerContainer(v, "cu11.8.0-cudnn8", "ray")
        assert container.get_platform_tag() == "-cu118"

        container = RayDockerContainer(v, "cu12.3.2-cudnn9", "ray")
        assert container.get_platform_tag() == "-cu123"

    def test_should_upload(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        test_cases = [
            # environment_variables, expected_result (with upload flag on)
            (
                {
                    "BUILDKITE_PIPELINE_ID": get_global_config()[
                        "ci_pipeline_postmerge"
                    ][0],
                    "BUILDKITE_BRANCH": "releases/1.0.0",
                },
                True,  # satisfy upload requirements
            ),
            (
                {
                    "BUILDKITE_PIPELINE_ID": get_global_config()[
                        "ci_pipeline_postmerge"
                    ][0],
                    "BUILDKITE_BRANCH": "master",
                    "RAYCI_SCHEDULE": "nightly",
                },
                True,  # satisfy upload requirements
            ),
            (
                {
                    "BUILDKITE_PIPELINE_ID": "123456",
                    "RAYCI_SCHEDULE": "nightly",
                    "BUILDKITE_BRANCH": "master",
                },
                False,  # not satisfied: pipeline is not postmerge
            ),
            (
                {
                    "BUILDKITE_PIPELINE_ID": get_global_config()[
                        "ci_pipeline_postmerge"
                    ][-1],
                    "BUILDKITE_BRANCH": "non-release/1.2.3",
                },
                False,  # not satisfied: branch is not release/master
            ),
            (
                {
                    "BUILDKITE_PIPELINE_ID": get_global_config()[
                        "ci_pipeline_postmerge"
                    ][-1],
                    "BUILDKITE_BRANCH": "123",
                    "RAYCI_SCHEDULE": "nightly",
                },
                False,  # not satisfied: branch is not master with nightly schedule
            ),
        ]
        # Run with upload flag on
        container = RayDockerContainer(v, "cpu", "ray", upload=True)
        for env_var, expected_result in test_cases:
            with mock.patch.dict(os.environ, env_var):
                assert container._should_upload() is expected_result

        # Run with upload flag off
        container = RayDockerContainer(v, "cpu", "ray", upload=False)
        for env_var, _ in test_cases:
            with mock.patch.dict(os.environ, env_var):
                assert container._should_upload() is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
