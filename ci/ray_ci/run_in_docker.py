import os

import click

from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.linux_container import LinuxContainer
from ci.ray_ci.utils import ci_init, ecr_docker_login


@click.command()
@click.argument("commands", required=True, nargs=-1)
@click.option("--build-name", required=True, help="Name of the Docker build image")
def main(commands, build_name):
    """Run commands in a CI build image."""
    ci_init()
    ecr_docker_login(_DOCKER_ECR_REPO.split("/")[0])
    checkout_dir = os.environ.get("RAYCI_CHECKOUT_DIR")
    volumes = []
    if checkout_dir:
        volumes.append(f"{checkout_dir}:/ray")
    container = LinuxContainer(build_name, volumes=volumes, workdir="/ray")
    container.run_script(list(commands))


if __name__ == "__main__":
    main()
