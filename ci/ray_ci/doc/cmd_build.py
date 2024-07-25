import subprocess
import os

import boto3
import click

from ci.ray_ci.utils import logger, ci_init
from ray_release.util import get_write_state_machine_aws_bucket
from ray_release.configs.global_config import get_global_config


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

    if (
        os.environ.get("BUILDKITE_PIPELINE_ID")
        not in get_global_config()["ci_pipeline_postmerge"]
    ):
        logger.info(
            "Not uploading build artifacts because this is not a postmerge pipeline."
        )
        return

    if os.environ.get("BUILDKITE_BRANCH") != "master":
        logger.info(
            "Not uploading build artifacts because this is not the master branch."
        )
        return

    logger.info("Uploading build artifacts to S3.")
    _upload_build_artifacts(ray_checkout_dir)

    return


def _build(ray_checkout_dir):
    env = os.environ.copy()
    # We need to unset PYTHONPATH to use the Python from the environment instead of
    # from the Bazel runfiles.
    env.update({"PYTHONPATH": ""})
    subprocess.run(
        ["make", "html"],
        cwd=os.path.join(ray_checkout_dir, "doc"),
        env=env,
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
