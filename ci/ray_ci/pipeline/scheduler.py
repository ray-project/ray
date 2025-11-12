import click

from ci.ray_ci.pipeline.gap_filling_scheduler import GapFillingScheduler
from ci.ray_ci.utils import ci_init, logger

from ray_release.aws import get_secret_token
from ray_release.configs.global_config import get_global_config


@click.command()
@click.argument("buildkite_organization", type=str)
@click.argument("buildkite_pipeline", type=str)
@click.argument("repo_checkout", type=str)
@click.option(
    "--days-ago",
    type=int,
    default=1,
    help=("Number of days ago to look for builds."),
)
@click.option(
    "--production",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Trigger builds in production."),
)
def main(
    buildkite_organization: str,
    buildkite_pipeline: str,
    repo_checkout: str,
    days_ago: int,
    production: bool,
) -> None:
    ci_init()
    scheduler = GapFillingScheduler(
        buildkite_organization,
        buildkite_pipeline,
        buildkite_access_token=get_secret_token(
            get_global_config()["ci_pipeline_buildkite_secret"],
        ),
        repo_checkout=repo_checkout,
        days_ago=days_ago,
    )
    if not production:
        logger.info("Dry run mode enabled. No builds will be created.")
        logger.info(f"Commits to be built: {scheduler.get_gap_commits()}")
        return

    builds = scheduler.run()
    logger.info(f"Created builds: {builds}")


if __name__ == "__main__":
    main()
