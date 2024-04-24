import sys
import os
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.anyscale_docker_container import AnyscaleDockerContainer
from ci.ray_ci.test_base import RayCITestBase
from ci.ray_ci.container import _DOCKER_GCP_REGISTRY, _DOCKER_ECR_REPO


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
            container = AnyscaleDockerContainer(v, "cu11.8.0", "ray-ml", upload=True)
            container.run()
            cmd = self.cmds[-1]
            aws_ecr = _DOCKER_ECR_REPO.split("/")[0]
            aws_prj = f"{aws_ecr}/anyscale/ray-ml"
            gcp_prj = f"{_DOCKER_GCP_REGISTRY}/anyscale/ray-ml"
            assert cmd == [
                "./ci/build/build-anyscale-docker.sh "
                f"rayproject/ray-ml:123456-{pv}-cu118 "
                f"{aws_prj}:123456-{pv}-cu118 requirements_ml_byod_{v}.txt {aws_ecr}",
                "./release/gcloud_docker_login.sh release/aws2gce_iam.json",
                "export PATH=$(pwd)/google-cloud-sdk/bin:$PATH",
                f"docker tag {aws_prj}:123456-{pv}-cu118 {aws_prj}:123456-{pv}-cu118",
                f"docker push {aws_prj}:123456-{pv}-cu118",
                f"docker tag {aws_prj}:123456-{pv}-cu118 {gcp_prj}:123456-{pv}-cu118",
                f"docker push {gcp_prj}:123456-{pv}-cu118",
                f"docker tag {aws_prj}:123456-{pv}-cu118 {aws_prj}:123456-{pv}-gpu",
                f"docker push {aws_prj}:123456-{pv}-gpu",
                f"docker tag {aws_prj}:123456-{pv}-cu118 {gcp_prj}:123456-{pv}-gpu",
                f"docker push {gcp_prj}:123456-{pv}-gpu",
                f"docker tag {aws_prj}:123456-{pv}-cu118 {aws_prj}:123456-{pv}",
                f"docker push {aws_prj}:123456-{pv}",
                f"docker tag {aws_prj}:123456-{pv}-cu118 {gcp_prj}:123456-{pv}",
                f"docker push {gcp_prj}:123456-{pv}",
            ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
