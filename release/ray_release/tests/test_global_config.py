import os
import sys
from tempfile import TemporaryDirectory

import pytest

from ray_release.configs.global_config import (
    get_global_config,
    init_global_config,
)

_TEST_CONFIG = """
byod:
  ray_cr_repo: ray
release_byod:
  ray_ml_cr_repo: ray-ml
  ray_llm_cr_repo: ray-llm
  byod_ecr: 029272617770.dkr.ecr.us-west-2.amazonaws.com
  byod_ecr_region: us-west-2
  gcp_cr: us-west1-docker.pkg.dev/anyscale-oss-ci
  azure_cr: rayreleasetest.azurecr.io
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
release_image_step:
  ray: anyscalebuild
  ray_ml: anyscalemlbuild
  ray_llm: anyscalellmbuild
"""


def test_init_global_config() -> None:
    with TemporaryDirectory() as tmp:
        config_file = os.path.join(tmp, "config")
        with open(config_file, "w") as f:
            f.write(_TEST_CONFIG)
        init_global_config(os.path.join(tmp, "config"))
        config = get_global_config()
        assert config["aws2gce_credentials"] == "release/aws2gce_iam.json"
        assert config["ci_pipeline_premerge"] == ["w00t"]
        assert config["ci_pipeline_postmerge"] == ["hi", "three"]
        assert (
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            == "/workdir/release/aws2gce_iam.json"
        )
        assert config["state_machine_pr_aws_bucket"] == "ray-ci-pr-results"
        assert config["state_machine_disabled"] is True
        assert config["release_image_step_ray"] == "anyscalebuild"
        assert config["release_image_step_ray_ml"] == "anyscalemlbuild"
        assert config["release_image_step_ray_llm"] == "anyscalellmbuild"
        assert config["byod_ecr"] == "029272617770.dkr.ecr.us-west-2.amazonaws.com"
        assert config["byod_ecr_region"] == "us-west-2"
        assert config["byod_gcp_cr"] == "us-west1-docker.pkg.dev/anyscale-oss-ci"
        assert config["byod_azure_cr"] == "rayreleasetest.azurecr.io"
        assert config["state_machine_branch_aws_bucket"] == "ray-ci-results"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
