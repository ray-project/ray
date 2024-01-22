import click

from ci.ray_ci.utils import logger
from ray_release.test_automation.state_machine import TestStateMachine


@click.command()
def main() -> None:
    blockers = TestStateMachine.get_release_blockers()
    logger.info(f"Found {blockers.totalCount} release blockers")

    blocker_teams = [TestStateMachine.get_issue_owner(blocker) for blocker in blockers]
    num_blocker_by_team = {team: blocker_teams.count(team) for team in blocker_teams}
    for team, num_blocker in num_blocker_by_team.items():
        logger.info(f"\t- Team {team} has {num_blocker} release blockers")


if __name__ == "__main__":
    main()
