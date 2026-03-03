import click

from ci.ray_ci.automation.docker_tags_lib import check_image_ray_commit
from ci.ray_ci.docker_container import RayType


@click.command()
@click.option("--ray_type", required=True, type=RayType)
@click.option("--expected_commit", required=True, type=str)
def main(ray_type, expected_commit):
    check_image_ray_commit("nightly", ray_type.value, expected_commit)


if __name__ == "__main__":
    main()
