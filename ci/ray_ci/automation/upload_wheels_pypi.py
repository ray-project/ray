import click
import tempfile

from ci.ray_ci.automation.ray_wheels_lib import download_ray_wheels_from_s3
from ci.ray_ci.automation.pypi_lib import upload_wheels_to_pypi


@click.command()
@click.option("--ray_version", required=True, type=str)
@click.option("--commit_hash", required=True, type=str)
@click.option("--pypi_env", required=True, type=click.Choice(["test", "prod"]))
def main(ray_version, commit_hash, pypi_env):
    with tempfile.TemporaryDirectory() as temp_dir:
        download_ray_wheels_from_s3(
            commit_hash=commit_hash, ray_version=ray_version, directory_path=temp_dir
        )
        upload_wheels_to_pypi(pypi_env=pypi_env, directory_path=temp_dir)


if __name__ == "__main__":
    main()
