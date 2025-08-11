import os
import sys
from typing import List
from unittest import mock

import pytest

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
            assert cmd == [
                "./ci/build/build-anyscale-docker.sh "
                f"rayproject/ray-ml:123456-{pv}-cu121 "
                f"{aws_prj}:123456-{pv}-cu121 requirements_ml_byod_{v}.txt {aws_ecr}",
                "./release/gcloud_docker_login.sh release/aws2gce_iam.json",
                "export PATH=$(pwd)/google-cloud-sdk/bin:$PATH",
                f"docker tag {aws_prj}:123456-{pv}-cu121 {aws_prj}:123456-{pv}-cu121",
                f"docker push {aws_prj}:123456-{pv}-cu121",
                f"docker tag {aws_prj}:123456-{pv}-cu121 {gcp_prj}:123456-{pv}-cu121",
                f"docker push {gcp_prj}:123456-{pv}-cu121",
                f"docker tag {aws_prj}:123456-{pv}-cu121 {aws_prj}:123456-{pv}-gpu",
                f"docker push {aws_prj}:123456-{pv}-gpu",
                f"docker tag {aws_prj}:123456-{pv}-cu121 {gcp_prj}:123456-{pv}-gpu",
                f"docker push {gcp_prj}:123456-{pv}-gpu",
                f"docker tag {aws_prj}:123456-{pv}-cu121 {aws_prj}:123456-{pv}",
                f"docker push {aws_prj}:123456-{pv}",
                f"docker tag {aws_prj}:123456-{pv}-cu121 {gcp_prj}:123456-{pv}",
                f"docker push {gcp_prj}:123456-{pv}",
            ]

    def test_requirements_file(self) -> None:
        container = AnyscaleDockerContainer("3.11", "cu12.1.1-cudnn8", "ray-ml")
        assert container._get_requirement_file() == "requirements_ml_byod_3.11.txt"

        container = AnyscaleDockerContainer("3.9", "cu12.1.1-cudnn8", "ray-ml")
        assert container._get_requirement_file() == "requirements_ml_byod_3.9.txt"

        container = AnyscaleDockerContainer("3.11", "cu12.4.1-cudnn", "ray-llm")
        assert container._get_requirement_file() == "requirements_llm_byod_3.11.txt"

        container = AnyscaleDockerContainer("3.9", "cpu", "ray")
        assert container._get_requirement_file() == "requirements_byod_3.9.txt"

        container = AnyscaleDockerContainer("3.12", "cpu", "ray")
        assert container._get_requirement_file() == "requirements_byod_3.12.txt"


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
