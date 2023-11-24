from ci.ray_ci.docker_container import DockerContainer
from ci.ray_ci.container import _DOCKER_ECR_REPO, _DOCKER_GCP_REGISTRY


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
        requirement = self._get_requirement_file()

        cmds = [
            # build docker image
            f"./ci/build/build-anyscale-docker.sh "
            f"{ray_image} {anyscale_image} {requirement} {aws_registry}",
            # gcloud login
            "./release/gcloud_docker_login.sh release/aws2gce_iam.json",
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

    def _get_requirement_file(self) -> str:
        prefix = "requirements" if self.image_type == "ray" else "requirements_ml"
        postfix = self.python_version

        return f"{prefix}_byod_{postfix}.txt"
