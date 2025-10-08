import os
import sys
from datetime import datetime
from typing import List
from unittest import mock

import pytest
from ray_release.configs.global_config import get_global_config

from ci.ray_ci.configs import DEFAULT_PYTHON_VERSION
from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.docker_container import GPU_PLATFORM
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
            sha = "123456"
            ray_ci_build_id = "a1b2c3d4"
            cuda = "cu12.4.1-cudnn"

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
                f"rayproject/ray:{sha}-{pv}-cu124 "
                f"ray:{sha}-{pv}-cu124_pip-freeze.txt"
            )

            # Run with specific python version and ray-llm image
            v = "3.11"
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            cuda = "cu12.8.1-cudnn"
            container = RayDockerContainer(v, cuda, "ray-llm")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-llm-py{v}-{cuda}-base "
                "requirements_compiled.txt "
                f"rayproject/ray-llm:{sha}-{pv}-cu128 "
                f"ray-llm:{sha}-{pv}-cu128_pip-freeze.txt"
            )

            # Run with non-default python version and ray-ml image
            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            cuda = "cu12.4.1-cudnn"
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
            ray_ci_build_id = "a1b2c3d4"

            # Run with default python version and ray image
            self.cmds = []
            v = DEFAULT_PYTHON_VERSION
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            cuda = "cu12.1.1-cudnn8"
            container = RayDockerContainer(v, cuda, "ray")
            container.run()
            assert len(self.cmds) == 18
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-py{v}-{cuda}-base "
                "requirements_compiled.txt "
                f"rayproject/ray:{sha}-{pv}-cu121 "
                f"ray:{sha}-{pv}-cu121_pip-freeze.txt"
            )
            assert (
                self.cmds[1]
                == "bazel run .buildkite:copy_files -- --destination docker_login"
            )
            for i in range(2, 10):  # check nightly.date.sha alias
                assert f"/ray:nightly.{formatted_date}.{sha}" in self.cmds[i]
            for i in range(10, len(self.cmds)):  # check nightly alias
                assert "/ray:nightly-" in self.cmds[i]

            # Run with specific python version and ray-llm image
            self.cmds = []
            v = "3.11"
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            cuda = "cu12.8.1-cudnn"
            container = RayDockerContainer(v, cuda, "ray-llm")
            container.run()
            assert len(self.cmds) == 6
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-llm-py{v}-{cuda}-base "
                "requirements_compiled.txt "
                f"rayproject/ray-llm:{sha}-{pv}-cu128 "
                f"ray-llm:{sha}-{pv}-cu128_pip-freeze.txt"
            )
            assert (
                self.cmds[1]
                == "bazel run .buildkite:copy_files -- --destination docker_login"
            )
            for i in range(2, 4):  # check nightly.date.sha alias
                assert f"/ray-llm:nightly.{formatted_date}.{sha}" in self.cmds[i]
            for i in range(4, len(self.cmds)):  # check nightly alias
                assert "/ray-llm:nightly-" in self.cmds[i]

            # Run with non-default python version and ray-ml image
            self.cmds = []
            v = self.get_non_default_python()
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, "cpu", "ray-ml")
            container.run()
            assert len(self.cmds) == 6
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-ml-py{v}-cpu-base "
                "requirements_compiled.txt "
                f"rayproject/ray-ml:{sha}-{pv}-cpu "
                f"ray-ml:{sha}-{pv}-cpu_pip-freeze.txt"
            )
            assert (
                self.cmds[1]
                == "bazel run .buildkite:copy_files -- --destination docker_login"
            )
            for i in range(2, 4):  # check nightly.date.sha alias
                assert f"/ray-ml:nightly.{formatted_date}.{sha}" in self.cmds[i]
            for i in range(4, len(self.cmds)):  # check nightly alias
                assert "/ray-ml:nightly-" in self.cmds[i]

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
            ray_ci_build_id = "a1b2c3d4"
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

            # Run with specific python version and ray-llm image
            self.cmds = []
            v = "3.11"
            cuda = "cu12.8.1-cudnn"
            cv = self.get_cpp_version(v)
            pv = self.get_python_version(v)
            container = RayDockerContainer(v, cuda, "ray-llm")
            container.run()
            assert len(self.cmds) == 1
            assert self.cmds[0] == (
                "./ci/build/build-ray-docker.sh "
                f"ray-{RAY_VERSION}-{cv}-{cv}-manylinux2014_x86_64.whl "
                f"{_DOCKER_ECR_REPO}:{ray_ci_build_id}-ray-llm-py{v}-{cuda}-base "
                "requirements_compiled.txt "
                f"rayproject/ray-llm:{sha}-{pv}-cu128 "
                f"ray-llm:{sha}-{pv}-cu128_pip-freeze.txt"
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
        rayci_build_id = "a1b2c3d4"
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
                f"{rayci_build_id}-{pv}-cpu",
                f"{rayci_build_id}-cpu",
                f"{rayci_build_id}-{pv}",
                f"{rayci_build_id}",
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
        rayci_build_id = "a1b2c3d4"
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
                f"rayproject/ray:{rayci_build_id}-{pv}-cpu",
                f"rayproject/ray:{rayci_build_id}-cpu",
                f"rayproject/ray:{rayci_build_id}-{pv}",
                f"rayproject/ray:{rayci_build_id}",
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

        container = RayDockerContainer(v, "cpu", "ray-extra")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_names() == [
                f"rayproject/ray:{sha}-extra-{pv}-cpu",
                f"rayproject/ray:{sha}-extra-cpu",
                f"rayproject/ray:{sha}-extra-{pv}",
                f"rayproject/ray:{sha}-extra",
                f"rayproject/ray:{rayci_build_id}-extra-{pv}-cpu",
                f"rayproject/ray:{rayci_build_id}-extra-cpu",
                f"rayproject/ray:{rayci_build_id}-extra-{pv}",
                f"rayproject/ray:{rayci_build_id}-extra",
            ]

        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "nightly"}):
            assert container._get_image_names() == [
                f"rayproject/ray:nightly.{formatted_date}.{sha}-extra-{pv}-cpu",
                f"rayproject/ray:nightly.{formatted_date}.{sha}-extra-cpu",
                f"rayproject/ray:nightly.{formatted_date}.{sha}-extra-{pv}",
                f"rayproject/ray:nightly.{formatted_date}.{sha}-extra",
                f"rayproject/ray:nightly-extra-{pv}-cpu",
                "rayproject/ray:nightly-extra-cpu",
                f"rayproject/ray:nightly-extra-{pv}",
                "rayproject/ray:nightly-extra",
            ]

        v = "3.11"
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cu12.8.1-cudnn", "ray-llm")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_names() == [
                f"rayproject/ray-llm:{sha}-{pv}-cu128",
                f"rayproject/ray-llm:{rayci_build_id}-{pv}-cu128",
            ]

        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "nightly"}):
            assert container._get_image_names() == [
                f"rayproject/ray-llm:nightly.{formatted_date}.{sha}-{pv}-cu128",
                f"rayproject/ray-llm:nightly-{pv}-cu128",
            ]

        container = RayDockerContainer(v, "cu12.8.1-cudnn", "ray-llm-extra")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_names() == [
                f"rayproject/ray-llm:{sha}-extra-{pv}-cu128",
                f"rayproject/ray-llm:{rayci_build_id}-extra-{pv}-cu128",
            ]

        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "nightly"}):
            assert container._get_image_names() == [
                f"rayproject/ray-llm:nightly.{formatted_date}.{sha}-extra-{pv}-cu128",
                f"rayproject/ray-llm:nightly-extra-{pv}-cu128",
            ]

        v = self.get_non_default_python()
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cu12.1.1-cudnn8", "ray-ml")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_names() == [
                f"rayproject/ray-ml:{sha}-{pv}-cu121",
                f"rayproject/ray-ml:{sha}-{pv}-gpu",
                f"rayproject/ray-ml:{sha}-{pv}",
                f"rayproject/ray-ml:{rayci_build_id}-{pv}-cu121",
                f"rayproject/ray-ml:{rayci_build_id}-{pv}-gpu",
                f"rayproject/ray-ml:{rayci_build_id}-{pv}",
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

        container = RayDockerContainer(v, "cu12.1.1-cudnn8", "ray-ml-extra")
        with mock.patch.dict(os.environ, {"RAYCI_SCHEDULE": "daytime"}):
            assert container._get_image_names() == [
                f"rayproject/ray-ml:{sha}-extra-{pv}-cu121",
                f"rayproject/ray-ml:{sha}-extra-{pv}-gpu",
                f"rayproject/ray-ml:{sha}-extra-{pv}",
                f"rayproject/ray-ml:{rayci_build_id}-extra-{pv}-cu121",
                f"rayproject/ray-ml:{rayci_build_id}-extra-{pv}-gpu",
                f"rayproject/ray-ml:{rayci_build_id}-extra-{pv}",
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
        assert container._get_python_version_tag() == f"-{pv}"

    def test_get_platform_tag(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        container = RayDockerContainer(v, "cpu", "ray")
        assert container._get_platform_tag() == "-cpu"

        container = RayDockerContainer(v, "cu11.8.0-cudnn8", "ray")
        assert container._get_platform_tag() == "-cu118"

        container = RayDockerContainer(v, "cu12.3.2-cudnn9", "ray")
        assert container._get_platform_tag() == "-cu123"

        container = RayDockerContainer(v, "cu12.4.1-cudnn", "ray")
        assert container._get_platform_tag() == "-cu124"

        container = RayDockerContainer(v, "cu12.5.1-cudnn", "ray")
        assert container._get_platform_tag() == "-cu125"

        container = RayDockerContainer(v, "cu12.6.3-cudnn", "ray")
        assert container._get_platform_tag() == "-cu126"

        container = RayDockerContainer(v, "cu12.8.1-cudnn", "ray")
        assert container._get_platform_tag() == "-cu128"

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
