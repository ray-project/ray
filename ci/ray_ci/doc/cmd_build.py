import subprocess
import os

import boto3
import click

from ci.ray_ci.utils import logger, ci_init
from ray_release.util import get_write_state_machine_aws_bucket


AWS_CACHE_KEY = "doc_build"


@click.command()
@click.option(
    "--ray-checkout-dir",
    default="/ray",
)
def main(ray_checkout_dir: str) -> None:
    """
    This script builds ray doc and upload build artifacts to S3.
    """
    ci_init()

    logger.info("Building ray doc.")
    _build(ray_checkout_dir)

    if os.environ.get("BUILDKITE_BRANCH") != "master":
        logger.info(
            "Not uploading build artifacts to S3 because this is not the master branch."
        )
        return

    logger.info("Uploading build artifacts to S3.")
    _upload_build_artifacts(ray_checkout_dir)

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


def _upload_build_artifacts(ray_checkout_dir):
    """
    Upload the build artifacts to S3.
    """
    # Get the list of the doc-generated files
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", ray_checkout_dir],
        check=True,
    )
    doc_generated_files = subprocess.check_output(
        ["git", "ls-files", "doc", "--others", "-z"],
        cwd=ray_checkout_dir,
    )

    # Create a tarball of the doc-generated files
    doc_tarball = f'{os.environ["BUILDKITE_COMMIT"]}.tgz'
    with subprocess.Popen(
        ["tar", "-cvzf", doc_tarball, "--null", "-T", "-"],
        stdin=subprocess.PIPE,
        cwd=ray_checkout_dir,
    ) as proc:
        proc.communicate(input=doc_generated_files)

    # Upload the tarball to S3
    boto3.client("s3").upload_file(
        os.path.join(ray_checkout_dir, doc_tarball),
        get_write_state_machine_aws_bucket(),
        f"{AWS_CACHE_KEY}/{doc_tarball}",
    )
    logger.info(f"Successfully uploaded {doc_tarball} to S3.")


if __name__ == "__main__":
    main()
