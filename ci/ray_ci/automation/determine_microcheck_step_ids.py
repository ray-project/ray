import click
from typing import List, Set, Dict

from ci.ray_ci.utils import logger, ci_init
from ray_release.configs.global_config import get_global_config
from ray_release.test import Test
from ray_release.result import ResultStatus

# The s3 prefix for the tests that run on Linux. It comes from the bazel prefix rule
# linux:// with the character "/" replaced by "_" for s3 compatibility
LINUX_TEST_PREFIX = "linux:__"


@click.command()
def main() -> None:
    """
    This script determines the rayci step ids to run microcheck tests.
    """
    ci_init()
    high_impact_tests = [
        test for test in Test.gen_from_s3(LINUX_TEST_PREFIX) test.is_high_impact()
    ]
    step_ids = [test.get_test_result(limit=1)[0].rayci_step_id for test in high_impact_tests]
    print(" ".join(step_ids))