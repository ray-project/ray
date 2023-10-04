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

        self.run_script(
            [
                f"./ci/build/build-anyscale-docker.sh {ray_image} {anyscale_image}",
            ]
        )
