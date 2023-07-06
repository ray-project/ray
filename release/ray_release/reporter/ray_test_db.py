import json

from ray_release.reporter.reporter import Reporter
from ray_release.result import Result, ResultStatus
from ray_release.test import Test
from ray_release.test_automation.state_machine import TestStateMachine
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
        if result.status == ResultStatus.TRANSIENT_INFRA_ERROR.value:
            logger.info(
                f"Skip recording result for test {test.get_name()} due to transient "
                "infra error result"
            )
            return
        logger.info(
            f"Updating test object {test.get_name()} with result {result.status}"
        )
        test.persist_result_to_s3(result)

        # Update the test object with the latest test state
        test.update_from_s3()
        logger.info(f"Test object: {json.dumps(test)}")
        logger.info(
            f"Test results: "
            f"{json.dumps([result.__dict__ for result in test.get_test_results()])}"
        )

        # Compute and update the next test state
        TestStateMachine(test).move()

        # Persist the updated test object to S3
        test.persist_to_s3()
        logger.info(f"Test object {test.get_name()} updated successfully")
