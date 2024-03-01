import os

import click

from ci.ray_ci.utils import logger
from ci.ray_ci.tester_container import TesterContainer, PIPELINE_POSTMERGE
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


@click.command()
@click.argument("team", required=True, type=str)
@click.argument("bazel_log_dir", required=True, type=str)
def main(team: str, bazel_log_dir: str) -> None:
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

    if os.environ.get("BUILDKITE_BRANCH") != "master":
        logger.info("Skip upload test results. We only upload on master branch.")
        return

    if os.environ.get("BUILDKITE_PIPELINE_ID") != PIPELINE_POSTMERGE:
        logger.info("Skip upload test results. We only upload on postmerge pipeline.")
        return

    TesterContainer.upload_test_results(team, bazel_log_dir)


if __name__ == "__main__":
    main()
