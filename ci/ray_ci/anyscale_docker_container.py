from ray_release.configs.global_config import get_global_config

from ci.ray_ci.container import _DOCKER_ECR_REPO, _DOCKER_GCP_REGISTRY
from ci.ray_ci.docker_container import DockerContainer


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
            "export PATH=$(pwd)/google-cloud-sdk/bin:$PATH",
        ]
        # TODO(can): remove the alias when release test infra uses only the canonical
        # tag
        if self._should_upload():
            for alias in self._get_image_tags():
                aws_alias_image = f"{aws_registry}/anyscale/{self.image_type}:{alias}"
                gcp_alias_image = f"{gcp_registry}/anyscale/{self.image_type}:{alias}"
                cmds += [
                    f"docker tag {anyscale_image} {aws_alias_image}",
                    f"docker push {aws_alias_image}",
                    f"docker tag {anyscale_image} {gcp_alias_image}",
                    f"docker push {gcp_alias_image}",
                ]

        self.run_script(cmds)

    def _should_upload(self) -> bool:
        return self.upload
