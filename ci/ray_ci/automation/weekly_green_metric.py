import json
import time
import sys

import boto3
import click

from ci.ray_ci.utils import logger, ci_init
from ray_release.test_automation.state_machine import TestStateMachine
from ray_release.util import get_write_state_machine_aws_bucket


AWS_WEEKLY_GREEN_METRIC = "ray_weekly_green_metric"


@click.command()
@click.option(
    "--production",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Persist the weekly green metric to S3."),
)
@click.option(
    "--check",
    is_flag=True,
    show_default=True,
    default=False,
    required=False,
    help=("Check whether there is 0 blockers."),
)
def main(production: bool, check: bool) -> None:
    ci_init()
    blockers = TestStateMachine.get_release_blockers()

    if production:
        logger.info(f"Found {len(blockers)} release blockers")
        blocker_teams = [
            TestStateMachine.get_issue_owner(blocker) for blocker in blockers
        ]
        num_blocker_by_team = {
            team: blocker_teams.count(team) for team in blocker_teams
        }
        for team, num_blocker in num_blocker_by_team.items():
            logger.info(f"\t- Team {team} has {num_blocker} release blockers")

        boto3.client("s3").put_object(
            Bucket=get_write_state_machine_aws_bucket(),
            Key=f"{AWS_WEEKLY_GREEN_METRIC}/blocker_{int(time.time() * 1000)}.json",
            Body=json.dumps(num_blocker_by_team),
        )
        logger.info("Weekly green metric updated successfully")

    if check:
        if len(blockers) > 0:
            print(
                f"Found {len(blockers)} release blockers.",
                file=sys.stderr,
            )
            for issue in blockers:
                print(f"{issue.html_url} - {issue.title}", file=sys.stderr)
            sys.exit(42)  # Not retrying the check on Buildkite jobs
        else:
            print("No release blockers. Woohoo!", file=sys.stderr)


if __name__ == "__main__":
    main()
