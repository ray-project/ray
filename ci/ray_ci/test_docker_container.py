import sys
from typing import List

import pytest
from unittest import mock
from ci.ray_ci.docker_container import DockerContainer
from ci.ray_ci.test_base import RayCITestBase


class TestDockerContainer(RayCITestBase):
    def test_run(self) -> None:
        def _mock_check_output(input: List[str]) -> None:
            input_str = " ".join(input)
            assert "ray-3.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl" in input_str
            assert "rayproject/citemp:123-raypy38cu118base" in input_str
            assert "requirements_compiled.txt" in input_str
            assert "rayproject/ray:123456-py38-cu118" in input_str

        with mock.patch(
            "ci.ray_ci.docker_container.docker_pull", return_value=None
        ), mock.patch("subprocess.check_output", side_effect=_mock_check_output):
            container = DockerContainer("py38")
            container.run()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
