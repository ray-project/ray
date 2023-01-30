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
    "default": (default.handle_result, default.REQ_NON_EMPTY_RESULT),
    "long_running_tests": (
        long_running_tests.handle_result,
        long_running_tests.REQ_NON_EMPTY_RESULT,
    ),
    "rllib_tests": (rllib_tests.handle_result, rllib_tests.REQ_NON_EMPTY_RESULT),
    "tune_tests": (tune_tests.handle_result, tune_tests.REQ_NON_EMPTY_RESULT),
    "xgboost_tests": (xgboost_tests.handle_result, xgboost_tests.REQ_NON_EMPTY_RESULT),
}


def require_non_empty_result(test: Test) -> bool:
    alert_suite = test.get("alert", "default")
    if alert_suite not in result_to_handle_map:
        raise ReleaseTestConfigError(f"Alert suite {alert_suite} not found.")
    return result_to_handle_map[alert_suite][1]


def handle_result(test: Test, result: Result):
    alert_suite = test.get("alert", "default")

    logger.info(
        f"Checking results for test {test['name']} using alerting suite "
        f"{alert_suite}"
    )

    if alert_suite not in result_to_handle_map:
        raise ReleaseTestConfigError(f"Alert suite {alert_suite} not found.")

    handler = result_to_handle_map[alert_suite][0]
    error = handler(test, result)

    if error:
        raise ResultsAlert(error)

    logger.info("No alerts have been raised - test passed successfully!")
