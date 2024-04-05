import os

import click

from ci.ray_ci.utils import logger, ci_init
from ci.ray_ci.tester_container import TesterContainer
from ray_release.configs.global_config import BRANCH_PIPELINES


@click.command()
@click.argument("team", required=True, type=str)
@click.argument("bazel_log_dir", required=True, type=str)
def main(team: str, bazel_log_dir: str) -> None:
    ci_init()

    if os.environ.get("BUILDKITE_BRANCH") != "master":
        logger.info("Skip upload test results. We only upload on master branch.")
        return

    if os.environ.get("BUILDKITE_PIPELINE_ID") not in BRANCH_PIPELINES:
        logger.info("Skip upload test results. We only upload on postmerge pipeline.")
        return

    TesterContainer.upload_test_results(team, bazel_log_dir)
    TesterContainer.move_test_state(team, bazel_log_dir)


if __name__ == "__main__":
    main()
