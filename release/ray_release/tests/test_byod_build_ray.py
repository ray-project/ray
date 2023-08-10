import pytest
import sys
from unittest.mock import patch
from typing import List

from ray_release.byod.build_ray import build_ray


def test_build_ray() -> None:
    cmds = []
    inputs = []

    def _mock_run(
        cmd: List[str],
        input: bytes,
        *args,
        **kwargs,
    ) -> None:
        cmds.append(cmd)
        inputs.append(input.decode("utf-8"))

    with patch.dict(
        "os.environ",
        {"BUILDKITE_PULL_REQUEST": "false"},
    ):
        build_ray()
        assert cmds == []

    with patch.dict(
        "os.environ",
        {"BUILDKITE_PULL_REQUEST": "1234", "BUILDKITE_COMMIT": "abcdef"},
    ), patch("subprocess.run", side_effect=_mock_run,), patch(
        "ray_release.byod.build_ray._base_image_exist",
        return_value=True,
    ):
        build_ray()
        assert cmds[0] == ["buildkite-agent", "pipeline", "upload"]
        assert "oss-ci-build_abcdef" in inputs[0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
