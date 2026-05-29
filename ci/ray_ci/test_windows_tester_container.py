from typing import List
from unittest import mock

from ci.ray_ci.windows_tester_container import WindowsTesterContainer


def test_init() -> None:
    install_ray_cmds = []

    def _mock_subprocess(inputs: List[str], stdout, stderr) -> None:
        install_ray_cmds.append(inputs)

    with mock.patch("subprocess.check_call", side_effect=_mock_subprocess):
        WindowsTesterContainer("hi")
        assert install_ray_cmds[-1] == [
            "docker",
            "build",
            "-t",
            "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:hi",
            "-f",
            "c:\\workdir\\ci\\ray_ci\\windows\\tests.env.Dockerfile",
            "c:\\workdir",
        ]
