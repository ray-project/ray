import os
import subprocess

import click
from ray_release.configs.global_config import get_global_config

from ci.ray_ci.doc.build_cache import BuildCache
from ci.ray_ci.utils import ci_init, logger


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
    # Add the safe.directory config to the global git config so that the doc build
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", ray_checkout_dir],
        check=True,
    )

    logger.info("Building ray doc.")
    _build(ray_checkout_dir)

    dry_run = False
    if (
        os.environ.get("BUILDKITE_PIPELINE_ID")
        not in get_global_config()["ci_pipeline_postmerge"]
    ):
        dry_run = True
        logger.info(
            "Not uploading build artifacts because this is not a postmerge pipeline."
        )

    if os.environ.get("BUILDKITE_BRANCH") != "master":
        dry_run = True
        logger.info(
            "Not uploading build artifacts because this is not the master branch."
        )

    logger.info("Uploading build artifacts to S3.")
    BuildCache(os.path.join(ray_checkout_dir, "doc")).upload(dry_run=dry_run)

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


if __name__ == "__main__":
    main()
