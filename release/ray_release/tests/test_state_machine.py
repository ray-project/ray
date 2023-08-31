import sys
from typing import List, Optional

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


class MockLabel:
    def __init__(self, name: str):
        self.name = name


class MockIssue:
    def __init__(
        self, number: int, state: str = "open", labels: Optional[List[MockLabel]] = None
    ):
        self.number = number
        self.state = state
        self.labels = labels or []
        self.comments = []

    def edit(self, state: str = None, labels: List[MockLabel] = None):
        if state:
            self.state = state
        if labels:
            self.labels = labels

    def create_comment(self, comment: str):
        self.comments.append(comment)

    def get_labels(self):
        return self.labels


class MockIssueDB:
    issue_id = 1
    issue_db = {}


class MockRepo:
    def create_issue(self, labels: List[str], *args, **kwargs):
        label_objs = [MockLabel(label) for label in labels]
        issue = MockIssue(MockIssueDB.issue_id, labels=label_objs)
        MockIssueDB.issue_db[MockIssueDB.issue_id] = issue
        MockIssueDB.issue_id += 1
        return issue

    def get_issue(self, number: int):
        return MockIssueDB.issue_db[number]


class MockBuildkiteBuild:
    def create_build(self, *args, **kwargs):
        return {
            "number": 1,
            "jobs": [{"id": "1"}],
        }

    def list_all_for_pipeline(self, *args, **kwargs):
        return []


class MockBuildkiteJob:
    def unblock_job(self, *args, **kwargs):
        return {}


class MockBuildkite:
    def builds(self):
        return MockBuildkiteBuild()

    def jobs(self):
        return MockBuildkiteJob()


TestStateMachine.ray_repo = MockRepo()
TestStateMachine.ray_buildkite = MockBuildkite()


def test_move_from_passing_to_failing():
    test = Test(name="test", team="ci")
    # Test original state
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value)),
    ]
    assert test.get_state() == TestState.PASSING

    # Test moving from passing to failing
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.FAILING
    assert test[Test.KEY_BISECT_BUILD_NUMBER] == 1

    # Test moving from failing to consistently failing
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    assert test[Test.KEY_GITHUB_ISSUE_NUMBER] == MockIssueDB.issue_id - 1


def test_move_from_failing_to_consisently_failing():
    test = Test(name="test", team="ci", stable=False)
    test[Test.KEY_BISECT_BUILD_NUMBER] = 1
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ]
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.FAILING
    test[Test.KEY_BISECT_BLAMED_COMMIT] = "1234567890"
    sm = TestStateMachine(test)
    sm.move()
    sm.comment_blamed_commit_on_github_issue()
    issue = MockIssueDB.issue_db[test.get(Test.KEY_GITHUB_ISSUE_NUMBER)]
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    assert "Blamed commit: 1234567890" in issue.comments[0]
    labels = [label.name for label in issue.get_labels()]
    assert "ci" in labels
    assert "unstable-release-test" in labels


def test_move_from_failing_to_passing():
    test = Test(name="test", team="ci")
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ]
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    assert test[Test.KEY_GITHUB_ISSUE_NUMBER] == MockIssueDB.issue_id - 1
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.PASSING
    assert test.get(Test.KEY_GITHUB_ISSUE_NUMBER) is None
    assert test.get(Test.KEY_BISECT_BUILD_NUMBER) is None
    assert test.get(Test.KEY_BISECT_BLAMED_COMMIT) is None


def test_move_from_failing_to_jailed():
    test = Test(name="test", team="ci")
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ]
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.JAILED

    # Test moving from jailed to jailed
    issue = MockIssueDB.issue_db[test.get(Test.KEY_GITHUB_ISSUE_NUMBER)]
    issue.edit(state="closed")
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.JAILED
    assert issue.state == "open"

    # Test moving from jailed to passing
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value)),
    )
    sm = TestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.PASSING
    assert test.get(Test.KEY_GITHUB_ISSUE_NUMBER) is None
    assert issue.state == "closed"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
