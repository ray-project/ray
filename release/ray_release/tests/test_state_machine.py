import sys

import pytest

from ray_release.test import (
    Test,
    TestResult,
    TestState,
)
from ray_release.result import (
    Result,
    ResultStatus,
)
from ray_release.test_automation.state_machine import TestStateMachine


def test_move():
    test = Test()
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.SUCCESS)),
    ]
    assert test.get_state() == TestState.PASSING
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR)),
    )
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.FAILING


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
