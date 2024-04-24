import click
import json

from ci.ray_ci.utils import logger, ci_init
from ci.ray_ci.bisect.macos_validator import MacOSValidator
from ci.ray_ci.bisect.bisector import Bisector
from ray_release.test import Test
from ray_release.test_automation.ci_state_machine import CITestStateMachine


TEST_PREFIXES = ["linux://", "darwin://", "windows://"]


@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    ci_init()
    test_target = _get_test_target(test_name)
    blame_commit = Bisector(
        test_target,
        passing_commit,
        failing_commit,
        MacOSValidator(),
    ).run()
    logger.info(f"Blame revision: {blame_commit}")
    _update_test_state(Test.gen_from_name(test_name), blame_commit)


def _get_test_target(test_name: str) -> str:
    for prefix in TEST_PREFIXES:
        if test_name.startswith(prefix):
            return test_name[len(prefix) :]
    return test_name


def _update_test_state(test: Test, blamed_commit: str) -> None:
    test.update_from_s3()
    logger.info(f"Test object: {json.dumps(test)}")
    test[Test.KEY_BISECT_BLAMED_COMMIT] = blamed_commit

    # Compute and update the next test state, then comment blamed commit on github issue
    sm = CITestStateMachine(test)
    sm.move()
    logger.info(f"Test object: {json.dumps(test)}")
    test.persist_to_s3()

    sm.comment_blamed_commit_on_github_issue()


if __name__ == "__main__":
    main()
