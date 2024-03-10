import os
import sys
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.builder_container import DEFAULT_PYTHON_VERSION
from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.ray_docker_container import RayDockerContainer
from ci.ray_ci.test_base import RayCITestBase
from ci.ray_ci.utils import RAY_VERSION, POSTMERGE_PIPELINE


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
        assert container._get_image_tags() == [
            f"123456-{pv}-cpu",
            "123456-cpu",
            f"123456-{pv}",
            "123456",
            f"nightly-{pv}-cpu",
            "nightly-cpu",
            f"nightly-{pv}",
            "nightly",
        ]

    def test_get_image_name(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cpu", "ray")
        assert container._get_image_names() == [
            f"rayproject/ray:123456-{pv}-cpu",
            "rayproject/ray:123456-cpu",
            f"rayproject/ray:123456-{pv}",
            "rayproject/ray:123456",
            f"rayproject/ray:nightly-{pv}-cpu",
            "rayproject/ray:nightly-cpu",
            f"rayproject/ray:nightly-{pv}",
            "rayproject/ray:nightly",
        ]

        v = self.get_non_default_python()
        pv = self.get_python_version(v)
        container = RayDockerContainer(v, "cu11.8.0", "ray-ml")
        assert container._get_image_names() == [
            f"rayproject/ray-ml:123456-{pv}-cu118",
            f"rayproject/ray-ml:123456-{pv}-gpu",
            f"rayproject/ray-ml:123456-{pv}",
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

    def test_should_upload(self) -> None:
        v = DEFAULT_PYTHON_VERSION
        test_cases = [
            # environment_variables, expected_result (with upload flag on)
            (
                {
                    "BUILDKITE_PIPELINE_ID": POSTMERGE_PIPELINE,
                    "BUILDKITE_BRANCH": "releases/1.0.0",
                },
                True,  # satisfy upload requirements
            ),
            (
                {
                    "BUILDKITE_PIPELINE_ID": POSTMERGE_PIPELINE,
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
                    "BUILDKITE_PIPELINE_ID": POSTMERGE_PIPELINE,
                    "BUILDKITE_BRANCH": "non-release/1.2.3",
                },
                False,  # not satisfied: branch is not release/master
            ),
            (
                {
                    "BUILDKITE_PIPELINE_ID": POSTMERGE_PIPELINE,
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
    sys.exit(pytest.main(["-v", __file__]))
