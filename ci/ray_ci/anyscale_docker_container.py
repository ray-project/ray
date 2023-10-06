from ci.ray_ci.docker_container import DockerContainer


class AnyscaleDockerContainer(DockerContainer):
    """
    Container for building and publishing anyscale docker images
    """

    def run(self) -> None:
        """
        Build and publish anyscale docker images
        """
        tag = self._get_canonical_tag()
        ray_image = f"rayproject/{self.image_type}:{tag}"
        anyscale_image = f"anyscale/{self.image_type}:{tag}"
        requirement = self._get_requirement_file()

        self.run_script(
            [
                f"./ci/build/build-anyscale-docker.sh "
                f"{ray_image} {anyscale_image} {requirement}",
            ]
        )

    def _get_requirement_file(self) -> str:
        prefix = "requirements" if self.image_type == "ray" else "requirements_ml"
        postfix = self.python_version

        return f"{prefix}_byod_{postfix}.txt"
