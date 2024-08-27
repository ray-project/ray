import os
import sys
from tempfile import TemporaryDirectory

import pytest

from ray_release.configs.global_config import (
    init_global_config,
    get_global_config,
)

_TEST_CONFIG = """
byod:
  ray_ecr: rayproject
  ray_cr_repo: ray
release_byod:
  ray_ml_cr_repo: ray-ml
  byod_ecr: 029272617770.dkr.ecr.us-west-2.amazonaws.com
  aws_cr: 029272617770.dkr.ecr.us-west-2.amazonaws.com
  gcp_cr: us-west1-docker.pkg.dev/anyscale-oss-ci
state_machine:
  pr:
    aws_bucket: ray-ci-pr-results
  branch:
    aws_bucket: ray-ci-results
  disabled: 1
credentials:
  aws2gce: release/aws2gce_iam.json
ci_pipeline:
  premerge:
    - w00t
  postmerge:
    - hi
    - three
"""


def test_init_global_config() -> None:
    with TemporaryDirectory() as tmp:
        config_file = os.path.join(tmp, "config")
        with open(config_file, "w") as f:
            f.write(_TEST_CONFIG)
        init_global_config(os.path.join(tmp, "config"))
        config = get_global_config()
        assert config["byod_ray_ecr"] == "rayproject"
        assert config["aws2gce_credentials"] == "release/aws2gce_iam.json"
        assert config["ci_pipeline_premerge"] == ["w00t"]
        assert config["ci_pipeline_postmerge"] == ["hi", "three"]
        assert (
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            == "/workdir/release/aws2gce_iam.json"
        )
        assert config["state_machine_pr_aws_bucket"] == "ray-ci-pr-results"
        assert config["state_machine_disabled"] is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
