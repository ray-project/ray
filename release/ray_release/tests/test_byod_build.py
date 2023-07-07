import sys

import pytest
from unittest.mock import patch
from typing import List

from ray_release.test import Test
from ray_release.byod.build import build_anyscale_custom_byod_image


def test_build_anyscale_custom_byod_image() -> None:
    cmds = []

    def _mock_check_call(
        cmd: List[str],
        *args,
        **kwargs,
    ) -> None:
        cmds.append(cmd)

    with patch(
        "ray_release.byod.build._byod_image_exist", return_value=False
    ), patch.dict(
        "os.environ",
        {"BUILDKITE_COMMIT": "abc123", "BUILDKITE_BRANCH": "master"},
    ), patch(
        "subprocess.check_call",
        side_effect=_mock_check_call,
    ):
        test = Test(
            name="name",
            cluster={"byod": {"post_build_script": "foo.sh"}},
        )
        build_anyscale_custom_byod_image(test)
        assert "docker build --build-arg BASE_IMAGE=029272617770.dkr.ecr.us-west-2."
        "amazonaws.com/anyscale/ray:abc123-py37 -t 029272617770.dkr.ecr.us-west-2."
        "amazonaws.com/anyscale/ray:abc123-py37-c3fc5fc6d84cea4d7ab885c6cdc966542e"
        "f59e4c679b8c970f2f77b956bfd8fb" in " ".join(cmds[0])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
