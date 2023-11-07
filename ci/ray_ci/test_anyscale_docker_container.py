import sys
import os
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.anyscale_docker_container import AnyscaleDockerContainer
from ci.ray_ci.test_base import RayCITestBase


class TestAnyscaleDockerContainer(RayCITestBase):
    cmds = []

    def test_run(self) -> None:
        def _mock_run_script(input: List[str]) -> None:
            self.cmds.append(input)

        with mock.patch(
            "ci.ray_ci.docker_container.Container.run_script",
            side_effect=_mock_run_script,
        ), mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "test_branch"}):
            container = AnyscaleDockerContainer("3.9", "cu11.8.0", "ray-ml")
            container.run()
            cmd = self.cmds[-1]
            ecr = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
            project = f"{ecr}/anyscale/ray-ml"
            assert cmd == [
                "./ci/build/build-anyscale-docker.sh "
                "rayproject/ray-ml:123456-py39-cu118 "
                f"{project}:123456-py39-cu118 requirements_ml_byod_3.9.txt {ecr}",
                f"docker tag {project}:123456-py39-cu118 {project}:123456-py39-cu118",
                f"docker push {project}:123456-py39-cu118",
                f"docker tag {project}:123456-py39-cu118 {project}:123456-py39-gpu",
                f"docker push {project}:123456-py39-gpu",
                f"docker tag {project}:123456-py39-cu118 {project}:123456-py39",
                f"docker push {project}:123456-py39",
            ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
