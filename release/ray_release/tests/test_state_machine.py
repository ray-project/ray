from ray_release.test import (
    Test,
    TestState,
)
from ray_release.result import (
    Result,
    ResultStatus,
)
from ray_release.test_automation.state_machine import TestStateMachine


def test_move():
    test = Test()
    test.test_results = []
    test.add_test_result(Result(status=ResultStatus.PASSING))
    assert test.get_state() == TestState.PASSING
    test.add_test_result(Result(status=ResultStatus.ERROR))
    test.add_test_result(Result(status=ResultStatus.ERROR))
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.FAILING
