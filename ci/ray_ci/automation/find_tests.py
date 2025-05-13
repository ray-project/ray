import asyncio
import click
from typing import List
from enum import Enum

from ci.ray_ci.utils import logger, ci_init
from ray_release.test import Test
from ray_release.test_automation.ci_state_machine import CITestStateMachine

# The s3 prefix for the tests that run on Linux. It comes from the bazel prefix rule
# linux:// with the character "/" replaced by "_" for s3 compatibility
MACOSX_TEST_PREFIX = "darwin:__"


class TestStatistics:
    @classmethod
    async def gen_analyze(
        cls, test: Test, test_history_length: int
    ) -> "TestStatistics":
        """
        Construct a TestStatistic object with statistic computed
        """
        stat = cls(test)
        stat._result_histories = test.get_test_results(
            limit=test_history_length, use_async=True
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
        return f"Test: {self.test.get_name()}, Flaky Percentage: {self.flaky_percentage:.2f}"

    def __init__(self, test: Test) -> None:
        self.test = test
        self.flaky_percentage = 0
        self._result_histories = []

    def _compute_flaky_percentage(self) -> float:
        self.flaky_percentage = CITestStateMachine.get_flaky_percentage(
            self._result_histories
        )


class OrderBy(str, Enum):
    """
    Enum for the order by options
    """

    FLAKY_PERCENTAGE = "flaky_percentage"


@click.command()
@click.argument("team", required=True, type=str)
@click.option("--test-history-length", default=30, type=int)
@click.option(
    "--test-prefix",
    default=MACOSX_TEST_PREFIX,
    type=str,
    help=(
        "The prefix of the test names to analyze. The default is darwin:__ which is the "
        "prefix of all the macOSX tests. Test names are the bazel target names, with "
        "the character '/' replaced by '_' for s3 compatibility."
    ),
)
@click.option(
    "--order-by",
    default=OrderBy.FLAKY_PERCENTAGE,
    type=click.Choice([OrderBy.FLAKY_PERCENTAGE]),
    help=(
        "Order either by the flaky percentage or some other metrics. The flaky "
        "percentage is computed in the same way as the CI test state machine does. "
        "It is the percentage of flaky transitions (FAILED to PASSED) in the test "
        "history."
    ),
)
@click.option("--debug", is_flag=True, default=False)
def main(
    team: str,
    test_history_length: int,
    test_prefix: str,
    order_by: str,
    debug: bool,
) -> None:
    logger.setLevel("INFO") if debug else logger.setLevel("WARNING")
    ci_init()
    tests = [
        test for test in Test.gen_from_s3(test_prefix) if test.get_oncall() == team
    ]
    print(f"Analyzing {len(tests)} tests for team {team}")
    test_stats = asyncio.run(gen_test_stats(tests, test_history_length))
    test_stats.sort(key=lambda x: x.get_flaky_percentage(), reverse=True)
    print(f"Tests sorted by {order_by}:")
    for test_stat in test_stats:
        print(f"\t - {test_stat}")


async def gen_test_stats(
    tests: List[Test],
    test_history_length: int,
) -> None:
    """
    This script finds tests that are low quality based on certain criteria (flakiness,
    slowness, etc.)
    """

    async def gen_analyze(test):
        stats = await TestStatistics.gen_analyze(test, test_history_length)
        logger.info(f"Got stats {stats}")
        return stats

    return await asyncio.gather(*[gen_analyze(test) for test in tests])


if __name__ == "__main__":
    main()
