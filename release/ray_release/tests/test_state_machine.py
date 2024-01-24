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
from ray_release.test_automation.release_state_machine import ReleaseTestStateMachine
from ray_release.test_automation.ci_state_machine import (
    CITestStateMachine,
    CONTINUOUS_FAILURE_TO_FLAKY,
    CONTINUOUS_PASSING_TO_PASSING,
    FAILING_TO_FLAKY_MESSAGE,
    JAILED_TAG,
    JAILED_MESSAGE,
)
from ray_release.test_automation.state_machine import (
    TestStateMachine,
    WEEKLY_RELEASE_BLOCKER_TAG,
    NO_TEAM,
)


class MockLabel:
    def __init__(self, name: str):
        self.name = name


class MockIssue:
    def __init__(
        self,
        number: int,
        title: str,
        state: str = "open",
        labels: Optional[List[MockLabel]] = None,
    ):
        self.number = number
        self.title = title
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


class MockUser:
    def get_issues(self, state: str, labels: List[MockLabel]) -> List[MockIssue]:
        issues = []
        for issue in MockIssueDB.issue_db.values():
            if issue.state != state:
                continue
            issue_labels = [label.name for label in issue.labels]
            if all(label.name in issue_labels for label in labels):
                issues.append(issue)

        return issues


class MockRepo:
    def create_issue(self, labels: List[str], title: str, *args, **kwargs):
        label_objs = [MockLabel(label) for label in labels]
        issue = MockIssue(MockIssueDB.issue_id, title=title, labels=label_objs)
        MockIssueDB.issue_db[MockIssueDB.issue_id] = issue
        MockIssueDB.issue_id += 1
        return issue

    def get_issue(self, number: int):
        return MockIssueDB.issue_db[number]

    def get_label(self, name: str):
        return MockLabel(name)


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


TestStateMachine.ray_user = MockUser()
TestStateMachine.ray_repo = MockRepo()
TestStateMachine.ray_buildkite = MockBuildkite()


def test_ci_move_from_passing_to_flaky():
    """
    Test the entire lifecycle of a CI test when it moves from passing to flaky.
    """
    test = Test(name="w00t", team="ci")
    # start from passing
    assert test.get_state() == TestState.PASSING

    # passing to flaky
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ] * 10
    CITestStateMachine(test).move()
    assert test.get_state() == TestState.FLAKY
    issue = MockIssueDB.issue_db[test.get(Test.KEY_GITHUB_ISSUE_NUMBER)]
    assert issue.state == "open"
    assert issue.title == "CI test w00t is flaky"

    # flaky to jail
    issue.edit(labels=[MockLabel(JAILED_TAG)])
    CITestStateMachine(test).move()
    assert test.get_state() == TestState.JAILED
    assert issue.comments[-1] == JAILED_MESSAGE


def test_ci_move_from_passing_to_failing_to_flaky():
    """
    Test the entire lifecycle of a CI test when it moves from passing to failing.
    Check that the conditions are met for each state transition. Also check that
    gihub issues are created and closed correctly.
    """
    test = Test(name="test", team="ci")
    # start from passing
    assert test.get_state() == TestState.PASSING

    # passing to failing
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ]
    CITestStateMachine(test).move()
    assert test.get_state() == TestState.FAILING

    # failing to consistently failing
    test.test_results.extend(
        [
            TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
            TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        ]
    )
    CITestStateMachine(test).move()
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    issue = MockIssueDB.issue_db[test.get(Test.KEY_GITHUB_ISSUE_NUMBER)]
    assert issue.state == "open"
    assert "ci-test" in [label.name for label in issue.labels]

    # move from consistently failing to flaky
    test.test_results.extend(
        [TestResult.from_result(Result(status=ResultStatus.ERROR.value))]
        * CONTINUOUS_FAILURE_TO_FLAKY
    )
    CITestStateMachine(test).move()
    assert test.get_state() == TestState.FLAKY
    assert issue.comments[-1] == FAILING_TO_FLAKY_MESSAGE

    # go back to passing
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value)),
    ] * CONTINUOUS_PASSING_TO_PASSING
    CITestStateMachine(test).move()
    assert test.get_state() == TestState.PASSING
    assert test.get(Test.KEY_GITHUB_ISSUE_NUMBER) is None
    assert issue.state == "closed"


def test_release_move_from_passing_to_failing():
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
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.FAILING
    assert test[Test.KEY_BISECT_BUILD_NUMBER] == 1

    # Test moving from failing to consistently failing
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    assert test[Test.KEY_GITHUB_ISSUE_NUMBER] == MockIssueDB.issue_id - 1


def test_release_move_from_failing_to_consisently_failing():
    test = Test(name="test", team="ci", stable=False)
    test[Test.KEY_BISECT_BUILD_NUMBER] = 1
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ]
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.FAILING
    test[Test.KEY_BISECT_BLAMED_COMMIT] = "1234567890"
    sm = ReleaseTestStateMachine(test)
    sm.move()
    sm.comment_blamed_commit_on_github_issue()
    issue = MockIssueDB.issue_db[test.get(Test.KEY_GITHUB_ISSUE_NUMBER)]
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    assert "Blamed commit: 1234567890" in issue.comments[0]
    labels = [label.name for label in issue.get_labels()]
    assert "ci" in labels
    assert "unstable-release-test" in labels


def test_release_move_from_failing_to_passing():
    test = Test(name="test", team="ci")
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ]
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    assert test[Test.KEY_GITHUB_ISSUE_NUMBER] == MockIssueDB.issue_id - 1
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value)),
    )
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.PASSING
    assert test.get(Test.KEY_GITHUB_ISSUE_NUMBER) is None
    assert test.get(Test.KEY_BISECT_BUILD_NUMBER) is None
    assert test.get(Test.KEY_BISECT_BLAMED_COMMIT) is None


def test_release_move_from_failing_to_jailed():
    test = Test(name="test", team="ci")
    test.test_results = [
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    ]
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.CONSITENTLY_FAILING
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.JAILED

    # Test moving from jailed to jailed
    issue = MockIssueDB.issue_db[test.get(Test.KEY_GITHUB_ISSUE_NUMBER)]
    issue.edit(state="closed")
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.ERROR.value)),
    )
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.JAILED
    assert issue.state == "open"

    # Test moving from jailed to passing
    test.test_results.insert(
        0,
        TestResult.from_result(Result(status=ResultStatus.SUCCESS.value)),
    )
    sm = ReleaseTestStateMachine(test)
    sm.move()
    assert test.get_state() == TestState.PASSING
    assert test.get(Test.KEY_GITHUB_ISSUE_NUMBER) is None
    assert issue.state == "closed"


def test_get_release_blockers() -> None:
    MockIssueDB.issue_id = 1
    MockIssueDB.issue_db = {}
    TestStateMachine.ray_repo.create_issue(labels=["non-blocker"], title="non-blocker")
    TestStateMachine.ray_repo.create_issue(
        labels=[WEEKLY_RELEASE_BLOCKER_TAG], title="blocker"
    )
    issues = TestStateMachine.get_release_blockers()
    assert len(issues) == 1
    assert issues[0].title == "blocker"


def test_get_issue_owner() -> None:
    issue = TestStateMachine.ray_repo.create_issue(labels=["core"], title="hi")
    assert TestStateMachine.get_issue_owner(issue) == "core"
    issue = TestStateMachine.ray_repo.create_issue(labels=["w00t"], title="bye")
    assert TestStateMachine.get_issue_owner(issue) == NO_TEAM


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
