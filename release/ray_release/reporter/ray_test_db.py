import json

from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.test import Test
from ray_release.logger import logger


class RayTestDBReporter(Reporter):
    """
    Reporter that updates the test and test result object in s3 with the latest test run
    information.
    - test: Test object that contains the test information (name, oncall, state, etc.)
    - result: Result object that contains the test result information of this particular
    test run (status, start time, end time, etc.)
    """

    def report_result(self, test: Test, result: Result) -> None:
        logger.info(
            f"Updating test object {test.get_name()} with result {result.status}"
        )
        test.persist_result_to_s3(result)
        logger.info(
            f"Test results: "
            f"{json.dumps([result.__dict__ for result in test.get_test_results()])}"
        )
        test.persist_to_s3()
        logger.info(f"Test object {test.get_name()} updated successfully")
