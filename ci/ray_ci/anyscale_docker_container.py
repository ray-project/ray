import os
import subprocess

from ci.ray_ci.container import (
    _AZURE_REGISTRY_NAME,
    _DOCKER_AZURE_REGISTRY,
    _DOCKER_ECR_REPO,
    _DOCKER_GCP_REGISTRY,
)
from ci.ray_ci.docker_container import DockerContainer

from ray_release.configs.global_config import get_global_config


class AnyscaleDockerContainer(DockerContainer):
    """
    Container for building and publishing anyscale docker images
    """

    def run(self) -> None:
        """
        Build and publish anyscale docker images
        """
        aws_registry = _DOCKER_ECR_REPO.split("/")[0]
        gcp_registry = _DOCKER_GCP_REGISTRY
        azure_registry = _DOCKER_AZURE_REGISTRY
        tag = self._get_canonical_tag()
        ray_image = f"rayproject/{self.image_type}:{tag}"
        anyscale_image = f"{aws_registry}/anyscale/{self.image_type}:{tag}"

        gce_credentials = get_global_config()["aws2gce_credentials"]
        cmds = [
            # build docker image
            "./ci/build/build-anyscale-docker.sh "
            + f"{ray_image} {anyscale_image} {aws_registry}",
            # gcloud login
            f"./release/gcloud_docker_login.sh {gce_credentials}",
            # azure login
            "./release/azure_docker_login.sh",
            # azure cr login
            f"az acr login --name {_AZURE_REGISTRY_NAME}",
            "export PATH=$(pwd)/google-cloud-sdk/bin:$PATH",
        ]
        # TODO(can): remove the alias when release test infra uses only the canonical
        # tag
        if self._should_upload():
            for alias in self._get_image_tags():
                aws_alias_image = f"{aws_registry}/anyscale/{self.image_type}:{alias}"
                gcp_alias_image = f"{gcp_registry}/anyscale/{self.image_type}:{alias}"
                azure_alias_image = (
                    f"{azure_registry}/anyscale/{self.image_type}:{alias}"
                )
                cmds += [
                    f"docker tag {anyscale_image} {aws_alias_image}",
                    f"docker push {aws_alias_image}",
                    f"docker tag {anyscale_image} {gcp_alias_image}",
                    f"docker push {gcp_alias_image}",
                    f"docker tag {anyscale_image} {azure_alias_image}",
                    f"docker push {azure_alias_image}",
                ]

            if os.environ.get("BUILDKITE"):
                subprocess.run(
                    [
                        "buildkite-agent",
                        "annotate",
                        "--style=info",
                        f"--context={self.image_type}-images",
                        "--append",
                        f"{aws_alias_image}<br/>",
                    ]
                )

        self.run_script(cmds)

    def _should_upload(self) -> bool:
        return self.upload
