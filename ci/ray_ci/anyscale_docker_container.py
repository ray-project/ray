from ci.ray_ci.docker_container import DockerContainer
from ci.ray_ci.container import _DOCKER_ECR_REPO


class AnyscaleDockerContainer(DockerContainer):
    """
    Container for building and publishing anyscale docker images
    """

    def run(self) -> None:
        """
        Build and publish anyscale docker images
        """
        ecr = _DOCKER_ECR_REPO.split("/")[0]
        tag = self._get_canonical_tag()
        ray_image = f"rayproject/{self.image_type}:{tag}"
        anyscale_image = f"{ecr}/anyscale/{self.image_type}:{tag}"
        requirement = self._get_requirement_file()

        cmds = [
            f"./ci/build/build-anyscale-docker.sh "
            f"{ray_image} {anyscale_image} {requirement} {ecr}",
        ]
        # TODO(can): remove the alias when release test infra uses only the canonical
        # tag
        for alias in self._get_image_tags():
            alias_image = f"{ecr}/anyscale/{self.image_type}:{alias}"
            cmds += [
                f"docker tag {anyscale_image} {alias_image}",
                f"docker push {alias_image}",
            ]

        self.run_script(cmds)

    def _get_requirement_file(self) -> str:
        prefix = "requirements" if self.image_type == "ray" else "requirements_ml"
        postfix = self.python_version

        return f"{prefix}_byod_{postfix}.txt"
