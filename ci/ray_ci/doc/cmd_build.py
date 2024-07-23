import subprocess

import click

from ci.ray_ci.utils import logger


@click.command()
@click.option(
    "--ray-checkout-dir",
    default="/ray",
)
def main(ray_checkout_dir: str) -> None:
    """
    This script builds ray doc and upload build artifacts to S3.
    """
    logger.info("Building ray doc.")
    _build(ray_checkout_dir)

    logger.info("Uploading build artifacts to S3.")
    _upload_build_artifacts()

    return


def _build(ray_checkout_dir):
    subprocess.run(
        [
            "make",
            "-C",
            "doc/",
            "html",
        ],
        cwd=ray_checkout_dir,
        check=True,
    )


def _upload_build_artifacts():
    """
    Upload the build artifacts to S3.

    TODO(can): to be implemented
    """
    pass


if __name__ == "__main__":
    main()
