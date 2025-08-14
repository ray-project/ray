import sys
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.builder_container import BuilderContainer


def test_init() -> None:
    builder = BuilderContainer("3.10", "optimized", "aarch64")
    assert builder.docker_tag == "manylinux-aarch64"
    builder = BuilderContainer("3.10", "optimized", "x86_64")
    assert builder.docker_tag == "manylinux"


def test_run() -> None:
    cmds = []

    def _mock_run_script(input: List[str]) -> None:
        cmds.append(input)

    with mock.patch(
        "ci.ray_ci.builder_container.BuilderContainer.run_script",
        side_effect=_mock_run_script,
    ):
        # test optimized build
        BuilderContainer("3.10", "optimized", "x86_64").run()
        assert cmds[-1] == [
            "./ci/build/build-manylinux-ray.sh",
            "./ci/build/build-manylinux-wheel.sh cp310-cp310",
            "chown -R 2000:100 /artifact-mount",
        ]

        # test debug build
        BuilderContainer("3.9", "debug", "x86_64").run()
        assert cmds[-1] == [
            "export RAY_DEBUG_BUILD=debug",
            "./ci/build/build-manylinux-ray.sh",
            "./ci/build/build-manylinux-wheel.sh cp39-cp39",
            "chown -R 2000:100 /artifact-mount",
        ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
