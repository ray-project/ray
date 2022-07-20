from ray_release.config import Test
from ray_release.exception import ReleaseTestConfigError, ResultsAlert
from ray_release.logger import logger
from ray_release.result import Result

from ray_release.alerts import (
    default,
    long_running_tests,
    rllib_tests,
    tune_tests,
    xgboost_tests,
)


result_to_handle_map = {
    "default": default.handle_result,
    "long_running_tests": long_running_tests.handle_result,
    "rllib_tests": rllib_tests.handle_result,
    "tune_tests": tune_tests.handle_result,
    "xgboost_tests": xgboost_tests.handle_result,
}


def handle_result(test: Test, result: Result):
    alert_suite = test.get("alert", "default")

    logger.info(
        f"Checking results for test {test['name']} using alerting suite "
        f"{alert_suite}"
    )

    if alert_suite not in result_to_handle_map:
        raise ReleaseTestConfigError(f"Alert suite {alert_suite} not found.")

    handler = result_to_handle_map[alert_suite]
    error = handler(test, result)

    if error:
        raise ResultsAlert(error)

    logger.info("No alerts have been raised - test passed successfully!")
