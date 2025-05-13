import click
from typing import List, Set, Dict
from enum import Enum

from ci.ray_ci.utils import logger, ci_init
from ray_release.configs.global_config import get_global_config
from ray_release.test import Test
from ray_release.result import ResultStatus
from ray_release.test_automation.ci_state_machine import CITestStateMachine

# The s3 prefix for the tests that run on Linux. It comes from the bazel prefix rule
# linux:// with the character "/" replaced by "_" for s3 compatibility
LINUX_TEST_PREFIX = "linux:__"

class TestStatistics:
    @classmethod
    def analyze(cls, test: Test, test_history_length: int) -> "TestStatistics":
        """
        Construct a TestStatistic object with statistic computed
        """
        stat = cls(test)
        stat._result_histories = test.get_test_results(
            limit=test_history_length,
            use_async=True,
        )
        stat._compute_flaky_percentage()

        return stat

    def get_flaky_percentage(self) -> float:
        """
        Get the flaky percentage of the test
        """
        return self.flaky_percentage

    def __str__(self) -> str:
        """
        String representation of the TestStatistics object
        """
        return f"Test: {self.test.get_name()}, Flaky Percentage: {self.flaky_percentage}"

    def __init__(self, test: Test) -> None:
        self.test = test
        self.flaky_percentage = 0
        self._result_histories = []

    def _compute_flaky_percentage(self, test_history_length: int) -> float:
        return CITestStateMachine.get_flaky_percentage(self._result_histories)

class OrderBy(str, Enum):
    """
    Enum for the order by options
    """
    FLAKY_PERCENTAGE = "flaky_percentage"


@click.command()
@click.argument("team", required=True, type=str)
@click.option("--test-history-length", default=100, type=int)
@click.option("--test-prefix", default=LINUX_TEST_PREFIX, type=str)
@click.option("--order-by", default=OrderBy.FLAKY_PERCENTAGE, type=click.Choice([OrderBy.FLAKY_PERCENTAGE]))
def main(
    team: str,
    test_history_length: int,
    test_prefix: str,
    order_by: str,
) -> None:
    """
    This script finds tests that are low quality based on certain criteria (flakiness, 
    slowness, etc.)
    """
    ci_init()
    tests = [
        test for test in Test.gen_from_s3(test_prefix) if test.get_oncall() == team
    ]
    logger.info(f"Analyzing {len(tests)} tests for team {team}")
    test_stats = ([
        TestStatistics.analyze(test, test_history_length) for test in tests
    ]).sort(
        key=lambda x: x.get_flaky_percentage(), reverse=True
    )
    logger.info(f"Tests sorted by {order_by}:")
    for test_stat in test_stats:
        logger.info(test_stat)


if __name__ == "__main__":
    main()
