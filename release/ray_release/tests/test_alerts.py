import sys
import pytest

from ray_release.alerts import (
    handle,
    default,
    # long_running_tests,
    # rllib_tests,
    # tune_tests,
    # xgboost_tests,
)
from ray_release.config import Test
from ray_release.exception import ReleaseTestConfigError, ResultsAlert
from ray_release.result import Result


def test_handle_alert():
    # Unknown test suite
    with pytest.raises(ReleaseTestConfigError):
        handle.handle_result(
            Test(name="unit_alert_test", alert="invalid"), Result(status="finished")
        )

    # Alert raised
    with pytest.raises(ResultsAlert):
        handle.handle_result(
            Test(name="unit_alert_test", alert="default"),
            Result(status="unsuccessful"),
        )

    # Everything fine
    handle.handle_result(
        Test(name="unit_alert_test", alert="default"), Result(status="finished")
    )


def test_default_alert():
    test = Test(name="unit_alert_test", alert="default")
    assert default.handle_result(test, Result(status="timeout"))
    assert not default.handle_result(test, Result(status="finished"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
