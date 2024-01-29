import json
import time

import boto3
import click

from ci.ray_ci.utils import logger
from ray_release.test_automation.state_machine import TestStateMachine
from ray_release.configs.global_config import init_global_config, get_global_config
from ray_release.bazel import bazel_runfile


AWS_WEEKLY_GREEN_METRIC = "ray_weekly_green_metric"


@click.command()
@click.option(
    "--production",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Persist the weekly green metric to S3."),
)
def main(production: bool) -> None:
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))
    blockers = TestStateMachine.get_release_blockers()
    logger.info(f"Found {blockers.totalCount} release blockers")

    blocker_teams = [TestStateMachine.get_issue_owner(blocker) for blocker in blockers]
    num_blocker_by_team = {team: blocker_teams.count(team) for team in blocker_teams}
    for team, num_blocker in num_blocker_by_team.items():
        logger.info(f"\t- Team {team} has {num_blocker} release blockers")

    if production:
        boto3.client("s3").put_object(
            Bucket=get_global_config()["state_machine_aws_bucket"],
            Key=f"{AWS_WEEKLY_GREEN_METRIC}/blocker_{int(time.time() * 1000)}.json",
            Body=json.dumps(num_blocker_by_team),
        )
        logger.info("Weekly green metric updated successfully")


if __name__ == "__main__":
    main()
