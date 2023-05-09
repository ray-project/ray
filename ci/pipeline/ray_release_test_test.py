import json
import sys
from unittest.mock import patch

import pytest

from ray_release_test import _get_test_targets_for_changed_files


@patch("ray_release_test._get_coverage_file", return_value=".coverage")
@patch("subprocess.check_call", return_value=None)
def test_get_test_targets_for_changed_files(mock_01, mock_02) -> None:
    with open("coverage.json", "w") as f:
        f.write(
            json.dumps(
                {
                    "files": {
                        "file_01.py": {
                            "contexts": {
                                1: [
                                    "release/tests/test_name.py::run",
                                    "release/tests/test_name2.py::run",
                                ],
                            }
                        },
                        "file_02.py": {
                            "contexts": {
                                1: [
                                    "release/tests/test_name3.py::run",
                                ],
                            }
                        },
                    }
                }
            )
        )
    assert _get_test_targets_for_changed_files(
        ["file_01.py", "file_02.py"], "tmp"
    ) == set(["//release:test_name", "//release:test_name2", "//release:test_name3"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
