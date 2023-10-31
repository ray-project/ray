import sys
import pytest
from unittest import mock
from typing import List

from ci.ray_ci.builder_container import BuilderContainer


def test_run() -> None:
    cmds = []

    def _mock_run_script(input: List[str]) -> None:
        cmds.append(input)

    with mock.patch(
        "ci.ray_ci.builder_container.BuilderContainer.run_script",
        side_effect=_mock_run_script,
    ):
        # test optimized build
        BuilderContainer("3.10", "optimized").run()
        assert cmds[-1] == [
            "./ci/build/build-manylinux-ray.sh",
            "./ci/build/build-manylinux-wheel.sh cp310-cp310 1.22.0",
            "chown -R 2000:100 /artifact-mount",
        ]

        # test debug build
        BuilderContainer("3.9", "debug").run()
        assert cmds[-1] == [
            "export RAY_DEBUG_BUILD=debug",
            "./ci/build/build-manylinux-ray.sh",
            "./ci/build/build-manylinux-wheel.sh cp39-cp39 1.19.3",
            "chown -R 2000:100 /artifact-mount",
        ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
