import os

import click
from ray_release.configs.global_config import get_global_config

from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.utils import ci_init, logger


@click.command()
@click.argument("team", required=True, type=str)
@click.argument("bazel_log_dir", required=True, type=str)
def main(team: str, bazel_log_dir: str) -> None:
    ci_init()
    pipeline = os.environ.get("BUILDKITE_PIPELINE_ID")
    postmerge_pipelines = get_global_config()["ci_pipeline_postmerge"]
    premerge_pipelines = get_global_config()["ci_pipeline_premerge"]

    if pipeline not in postmerge_pipelines + premerge_pipelines:
        logger.info(
            "Skip upload test results. "
            "We only upload on premerge or postmerge pipeline."
        )
        return

    if (
        pipeline in postmerge_pipelines
        and os.environ.get("BUILDKITE_BRANCH") != "master"
    ):
        logger.info(
            "Skip upload test results. "
            "We only upload on the master branch on postmerge pipeline."
        )
        return

    TesterContainer.upload_test_results(team, bazel_log_dir)
    TesterContainer.move_test_state(team, bazel_log_dir)


if __name__ == "__main__":
    main()
