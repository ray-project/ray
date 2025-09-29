import os
import sys
from typing import List
from unittest import mock

import pytest
from ray_release.configs.global_config import get_global_config

from ci.ray_ci.anyscale_docker_container import AnyscaleDockerContainer
from ci.ray_ci.container import _DOCKER_ECR_REPO, _DOCKER_GCP_REGISTRY
from ci.ray_ci.test_base import RayCITestBase


class TestAnyscaleDockerContainer(RayCITestBase):
    cmds = []

    def test_run(self) -> None:
        def _mock_run_script(input: List[str]) -> None:
            self.cmds.append(input)

        v = self.get_non_default_python()
        pv = self.get_python_version(v)

        with mock.patch(
            "ci.ray_ci.docker_container.LinuxContainer.run_script",
            side_effect=_mock_run_script,
        ), mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "test_branch"}):
            container = AnyscaleDockerContainer(
                v, "cu12.1.1-cudnn8", "ray-ml", upload=True
            )
            container.run()
            cmd = self.cmds[-1]
            aws_ecr = _DOCKER_ECR_REPO.split("/")[0]
            aws_prj = f"{aws_ecr}/anyscale/ray-ml"
            gcp_prj = f"{_DOCKER_GCP_REGISTRY}/anyscale/ray-ml"
            gce_credentials = get_global_config()["aws2gce_credentials"]

            tags_want = [
                f"123456-{pv}-cu121",
                f"123456-{pv}-gpu",
                f"123456-{pv}",
                f"a1b2c3d4-{pv}-cu121",
                f"a1b2c3d4-{pv}-gpu",
                f"a1b2c3d4-{pv}",
            ]

            push_cmds_want = []
            for tag in tags_want:
                push_cmds_want += [
                    f"docker tag {aws_prj}:123456-{pv}-cu121 {aws_prj}:{tag}",
                    f"docker push {aws_prj}:{tag}",
                    f"docker tag {aws_prj}:123456-{pv}-cu121 {gcp_prj}:{tag}",
                    f"docker push {gcp_prj}:{tag}",
                ]

            assert (
                cmd
                == [
                    "./ci/build/build-anyscale-docker.sh "
                    f"rayproject/ray-ml:123456-{pv}-cu121 "
                    f"{aws_prj}:123456-{pv}-cu121 {aws_ecr}",
                    f"./release/gcloud_docker_login.sh {gce_credentials}",
                    "export PATH=$(pwd)/google-cloud-sdk/bin:$PATH",
                ]
                + push_cmds_want
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
