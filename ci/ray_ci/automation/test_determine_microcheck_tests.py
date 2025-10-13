import json
import sys
from typing import List

import pytest
from ray_release.result import ResultStatus
from ray_release.test import Test, TestResult

from ci.ray_ci.automation.determine_microcheck_tests import (
    _get_failed_commits,
    _get_failed_tests_from_master_branch,
    _get_flaky_tests,
    _get_test_with_minimal_coverage,
    _update_high_impact_tests,
)
from ci.ray_ci.utils import ci_init

ci_init()

DB = {}


class MockTest(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_name(self) -> str:
        return self.get("name", "")

    def get_test_results(
        self, limit: int, aws_bucket: str, use_async: bool, refresh: bool
    ) -> List[TestResult]:
        return self.get("test_results", [])

    def update_from_s3(self) -> None:
        pass

    def persist_to_s3(self) -> None:
        DB[self["name"]] = json.dumps(self)


def stub_test_result(status: ResultStatus, branch: str, commit: str = "") -> TestResult:
    return TestResult(
        status=status.value,
        branch=branch,
        commit=commit,
        url="",
        timestamp=0,
        pull_request="",
        rayci_step_id="",
    )


def test_update_high_impact_tests():
    tests = [
        MockTest(
            {
                "name": "good_test",
                Test.KEY_IS_HIGH_IMPACT: "false",
            }
        ),
        MockTest(
            {
                "name": "bad_test",
                Test.KEY_IS_HIGH_IMPACT: "false",
            }
        ),
    ]
    _update_high_impact_tests(tests, {"good_test"})
    assert json.loads(DB["good_test"])[Test.KEY_IS_HIGH_IMPACT] == "true"
    assert json.loads(DB["bad_test"])[Test.KEY_IS_HIGH_IMPACT] == "false"


def test_get_failed_commits():
    assert _get_failed_commits(
        MockTest(
            {
                "name": "test",
                "test_results": [
                    stub_test_result(ResultStatus.ERROR, "w00t", commit="1w00t2"),
                    stub_test_result(ResultStatus.ERROR, "w00t", commit="2w00t3"),
                    stub_test_result(ResultStatus.SUCCESS, "hi", commit="5hi7"),
                    stub_test_result(ResultStatus.ERROR, "f00", commit="1f003"),
                ],
            }
        ),
        1,
    ) == {"1w00t2", "2w00t3", "1f003"}


def test_get_failed_tests_from_master_branch():
    failed_test_01 = MockTest(
        {
            "name": "test_01",
            "test_results": [
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
            ],
        },
    )
    failed_test_02 = MockTest(
        {
            "name": "test_02",
            "test_results": [
                stub_test_result(ResultStatus.ERROR, "non_master"),
                stub_test_result(ResultStatus.SUCCESS, "non_master"),
                stub_test_result(ResultStatus.ERROR, "non_master"),
                stub_test_result(ResultStatus.ERROR, "non_master"),
            ],
        },
    )
    failed_test_03 = MockTest(
        {
            "name": "test_03",
            "test_results": [
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
            ],
        },
    )
    _get_failed_tests_from_master_branch(
        [failed_test_01, failed_test_02, failed_test_03], 2
    ) == {"test_01"}


def test_get_flaky_tests():
    good_test = MockTest(
        {
            "name": "good_test",
            "test_results": [
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
            ],
        },
    )
    flaky_test = MockTest(
        {
            "name": "flaky_test",
            "test_results": [
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
            ],
        },
    )
    bad_test = MockTest(
        {
            "name": "flaky_test",
            "test_results": [
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.ERROR, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
                stub_test_result(ResultStatus.SUCCESS, "master"),
            ],
        },
    )
    assert _get_flaky_tests([good_test, flaky_test, bad_test], 2) == {"flaky_test"}


def test_get_test_with_minimal_coverage():
    # empty cases
    assert _get_test_with_minimal_coverage({}, {}, 50) == set()

    # normal cases
    test_to_prs = {
        "test1": {"a"},
        "test2": {"a", "b"},
        "test3": {"c"},
        "test4": {"d"},
    }
    assert _get_test_with_minimal_coverage(test_to_prs, {"test2"}, 0) == set()
    assert _get_test_with_minimal_coverage(test_to_prs, {"test2"}, 50) == {
        "test1",
        "test3",
    }
    assert _get_test_with_minimal_coverage(test_to_prs, {"test2"}, 75) == {
        "test1",
        "test3",
        "test4",
    }

    # one beat all cases
    test_to_prs = {
        "test1": {"a"},
        "test2": {"a", "b"},
        "test3": {"a", "b", "c"},
    }
    assert _get_test_with_minimal_coverage(test_to_prs, {}, 50) == {"test3"}
    assert _get_test_with_minimal_coverage(test_to_prs, {}, 75) == {"test3"}

    # equal distribution cases
    test_to_prs = {
        "test1": {"a"},
        "test2": {"b"},
        "test3": {"c"},
    }
    assert _get_test_with_minimal_coverage(test_to_prs, {}, 100) == {
        "test1",
        "test2",
        "test3",
    }

    # one beat all but flaky test cases
    test_to_prs = {
        "test1": {"a"},
        "test2": {"a", "b"},
        "test3": {"a", "b", "c"},
    }
    assert _get_test_with_minimal_coverage(test_to_prs, {"test3"}, 50) == {"test2"}
    assert _get_test_with_minimal_coverage(test_to_prs, {"test3"}, 75) == {
        "test2",
        "test3",
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
