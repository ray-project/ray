from ray_release.test import Test
from ray_release.exception import ReleaseTestConfigError, ResultsAlert
from ray_release.logger import logger
from ray_release.result import Result

from ray_release.alerts import (
    default,
    long_running_tests,
    tune_tests,
    xgboost_tests,
)


# The second bit in the tuple indicates whether a result is required to pass the alert.
# If true, the release test will throw a FetchResultError when result cannot be fetched
# successfully.
result_to_handle_map = {
    "default": (default.handle_result, False),
    "long_running_tests": (
        long_running_tests.handle_result,
        True,
    ),
    "tune_tests": (tune_tests.handle_result, True),
    "xgboost_tests": (xgboost_tests.handle_result, True),
}


def require_result(test: Test) -> bool:
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
