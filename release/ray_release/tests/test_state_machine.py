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


class MockIssue:
    def __init__(self, number: int):
        self.number = number


class MockRepo:
    def create_issue(self, *args, **kwargs):
        return MockIssue(10)


TestStateMachine.ray_repo = MockRepo()


def test_move_from_passing_to_failing():
    test = Test(name="test", team="devprod")
    # Test original state
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value))
    ]
    assert test.get_state() == TestState.PASSING
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.FAILING

    # Test github issue is created
    assert test[Test.KEY_GITHUB_ISSUE_NUMBER] == 10


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
