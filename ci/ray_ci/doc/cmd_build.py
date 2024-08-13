import subprocess
import os

import click

from ci.ray_ci.utils import logger, ci_init
from ci.ray_ci.doc.build_cache import BuildCache

from ray_release.configs.global_config import get_global_config


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
    BuildCache(os.path.join(ray_checkout_dir, "doc")).upload()

    return


def _build(ray_checkout_dir):
    env = os.environ.copy()
    # We need to unset PYTHONPATH to use the Python from the environment instead of
    # from the Bazel runfiles.
    env.update({"PYTHONPATH": ""})
    try:
        import numpy

        logger.info(
            f"numpy version: {numpy.__version__}, type: {type(numpy.__version__)}"
        )
        logger.info(f"test = {numpy.__version__ < '2'}")
    except Exception as e:
        logger.error(f"Failed to import numpy: {e}")
    subprocess.run(
        ["make", "html"],
        cwd=os.path.join(ray_checkout_dir, "doc"),
        env=env,
        check=True,
    )


if __name__ == "__main__":
    main()
