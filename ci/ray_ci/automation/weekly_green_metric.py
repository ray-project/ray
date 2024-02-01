import click

from ci.ray_ci.utils import logger
from ray_release.test_automation.state_machine import TestStateMachine


@click.command()
def main() -> None:
    blockers = TestStateMachine.get_release_blockers()
    logger.info(f"Found {blockers.totalCount} release blockers")


if __name__ == "__main__":
    main()
