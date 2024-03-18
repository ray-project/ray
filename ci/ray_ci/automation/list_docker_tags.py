import click
import sys

from ci.ray_ci.automation.docker_tags_lib import list_docker_image_versions

@click.command()
@click.option("--prefix", required=True, type=str)
@click.option("--ray_type", required=True, type=click.Choice(["ray", "ray-ml"]))
def main(prefix, ray_type):
    tags = list_docker_image_versions(prefix, ray_type)
    output = sys.stdout
    for tag in tags:
        output.write(tag + "\n")

if __name__ == "__main__":
    main()