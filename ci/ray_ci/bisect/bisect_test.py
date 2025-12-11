import json
import os

import click
from ray_release.test import (
    Test,
    TestType,
)
from ray_release.test_automation.ci_state_machine import CITestStateMachine

from ci.ray_ci.bisect.bisector import Bisector
from ci.ray_ci.bisect.generic_validator import GenericValidator
from ci.ray_ci.bisect.macos_validator import MacOSValidator
from ci.ray_ci.utils import ci_init, logger

# This is the directory where the ray repository is mounted in the container
RAYCI_CHECKOUT_DIR_MOUNT = "/ray"


@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    ci_init()
    test = Test.gen_from_name(test_name)
    if test.get_test_type() == TestType.MACOS_TEST:
        validator = MacOSValidator()
        git_dir = os.environ.get("RAYCI_CHECKOUT_DIR")
    else:
        validator = GenericValidator()
        git_dir = RAYCI_CHECKOUT_DIR_MOUNT
    blame_commit = Bisector(
        test,
        passing_commit,
        failing_commit,
        validator,
        git_dir,
    ).run()
    logger.info(f"Blame revision: {blame_commit}")
    _update_test_state(test, blame_commit)


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
