import pytest
import sys
from unittest.mock import patch
from typing import List

from ray_release.byod.build_ray import build_ray, _get_py_and_cuda_versions
from ray_release.test import Test


def test__get_py_and_cuda_versions() -> None:
    assert _get_py_and_cuda_versions(
        [
            Test(
                {
                    "name": "test01",
                    "python": "3.9",
                    "cluster": {
                        "byod": {
                            "type": "gpu",
                        }
                    },
                }
            ),
            Test(
                {
                    "name": "test02",
                    "python": "3.9",
                    "cluster": {
                        "byod": {
                            "type": "cpu",
                        }
                    },
                }
            ),
            Test(
                {
                    "name": "test03",
                    "cluster": {"byod": {}},
                }
            ),
        ]
    ) == {
        "py39": {"cu118", "cpu"},
        "py38": {"cpu"},
    }


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
        build_ray([])
        assert cmds == []

    with patch.dict(
        "os.environ",
        {"BUILDKITE_PULL_REQUEST": "1234", "BUILDKITE_COMMIT": "abcdef"},
    ), patch("subprocess.run", side_effect=_mock_run,), patch(
        "ray_release.byod.build_ray._base_image_exist",
        return_value=True,
    ):
        build_ray(
            [
                Test(
                    {
                        "name": "test",
                        "python": "3.9",
                        "cluster": {
                            "byod": {
                                "type": "gpu",
                            }
                        },
                    }
                ),
            ]
        )
        assert cmds[0] == ["buildkite-agent", "pipeline", "upload"]
        assert "oss-ci-build_abcdef" in inputs[0]
        assert "--py-versions py39 -T cu118" in inputs[0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
