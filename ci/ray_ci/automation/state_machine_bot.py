import click
from typing import List

from ci.ray_ci.utils import logger
from ray_release.test import Test
from ray_release.test_automation.ci_state_machine import CITestStateMachine


@click.command()
@click.option(
    "--production",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Update the state of the test on S3 and perform state transition actions."),
)
def main(production: bool) -> None:
    """
    Run state machine on all CI tests.
    """
    logger.info("Starting state machine bot ...")
    tests = _get_all_ci_tests()
    for test in tests:
        _move_state(test, production)


def _move_state(test, production: bool) -> None:
    """
    Run state machine on all CI tests.
    """
    logger.info(f"Running state machine on test {test.get_name()} ...")
    test.update_from_s3()
    logger.info(f"\tOld state: {test.get_state()}")

    CITestStateMachine(test, dry_run=not production).move()
    logger.info(f"\tNew state: {test.get_state()}")

    if not production:
        return

    test.persist_to_s3()
    logger.info("\tTest state updated successfully")


def _get_all_ci_tests() -> List[Test]:
    """
    Get all CI tests.
    """
    return []


if __name__ == "__main__":
    main()
