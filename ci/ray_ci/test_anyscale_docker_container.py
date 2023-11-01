import sys
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
        ):
            container = AnyscaleDockerContainer("3.8", "cu11.8.0", "ray")
            container.run()
            cmd = self.cmds[-1]
            assert cmd == [
                "./ci/build/build-anyscale-docker.sh rayproject/ray:123456-py38-cu118 "
                "029272617770.dkr.ecr.us-west-2.amazonaws.com"
                "/anyscale/ray:123456-py38-cu118 requirements_byod_3.8.txt "
                "029272617770.dkr.ecr.us-west-2.amazonaws.com",
            ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
