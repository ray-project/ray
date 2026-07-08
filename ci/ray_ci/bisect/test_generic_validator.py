import os
import sys
import time
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from ci.ray_ci.bisect.generic_validator import WAIT, GenericValidator

from ray_release.configs.global_config import init_global_config
from ray_release.test import Test

_VALIDATOR_TEST_CONFIG = """
release_byod:
  byod_ecr: 029272617770.dkr.ecr.us-west-2.amazonaws.com
  byod_ecr_region: us-west-2
  gcp_cr: us-west1-docker.pkg.dev/anyscale-oss-ci
  aws2gce_credentials: release/aws2gce_iam.json
ci_pipeline:
  buildkite_org: ray-project
  postmerge:
    - hi
"""

_validator_tmp = TemporaryDirectory()
_validator_cfg = os.path.join(_validator_tmp.name, "config")
with open(_validator_cfg, "w") as _f:
    _f.write(_VALIDATOR_TEST_CONFIG)
# GenericValidator.run reads the buildkite org from the global config.
init_global_config(_validator_cfg)

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
