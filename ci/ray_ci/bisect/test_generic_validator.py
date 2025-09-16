import sys
import time
from unittest import mock

import pytest
from ray_release.test import Test

from ci.ray_ci.bisect.generic_validator import WAIT, GenericValidator

START = time.time()


class MockBuildkiteBuild:
    def create_build(self, *args, **kwargs):
        return {
            "number": 1,
            "state": "creating",
        }

    def get_build_by_number(self, *args, **kwargs):
        # Simulate a build that takes 2 cycle of WAIT to pass
        build = self.create_build()
        if time.time() - START > 2 * WAIT:
            build["state"] = "passed"
        else:
            build["state"] = "running"

        return build


class MockBuildkite:
    def builds(self):
        return MockBuildkiteBuild()


@mock.patch("ci.ray_ci.bisect.generic_validator.GenericValidator._get_buildkite")
@mock.patch("ci.ray_ci.bisect.generic_validator.GenericValidator._get_rayci_select")
def test_run(mock_get_rayci_select, mock_get_buildkite):
    mock_get_rayci_select.return_value = "rayci_step_id"
    mock_get_buildkite.return_value = MockBuildkite()
    assert GenericValidator().run(Test({"name": "test"}), "revision")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
