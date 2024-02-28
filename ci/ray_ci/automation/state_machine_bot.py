from typing import List

import click

from ci.ray_ci.utils import logger
from ray_release.test import Test
from ray_release.test_automation.ci_state_machine import CITestStateMachine
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


ALL_TEST_PREFIXES = ["linux:", "windows:", "darwin:"]


@click.command()
@click.option(
    "--production",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Update the state of the test on S3 and perform state transition actions."),
)
@click.option(
    "--test-prefix",
    multiple=True,
    type=str,
    help=(
        "Prefix of tests to run the state machine on. "
        "If not specified, run on all tests."
    ),
)
def main(production: bool, test_prefix: List[str]) -> None:
    """
    Run state machine on all CI tests.
    """
    logger.info("Starting state machine bot ...")
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))
    test_prefix = test_prefix or ALL_TEST_PREFIXES
    tests = _get_ci_tests(test_prefix)
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


def _get_ci_tests(prefixes: List[str]) -> List[Test]:
    """
    Get all CI tests.
    """
    tests = []
    for prefix in prefixes:
        tests.extend(Test.gen_from_s3(prefix))

    return tests


if __name__ == "__main__":
    main()
