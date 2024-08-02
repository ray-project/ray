import click
import tempfile

from ci.ray_ci.automation.ray_wheels_lib import check_wheels_exist_on_s3


@click.command()
@click.option("--ray_version", required=True, type=str)
@click.option("--commit_hash", required=True, type=str)
def main(ray_version: str, commit_hash: str):
    check_wheels_exist_on_s3(
        commit_hash=commit_hash, ray_version=ray_version
    )


if __name__ == "__main__":
    main()
