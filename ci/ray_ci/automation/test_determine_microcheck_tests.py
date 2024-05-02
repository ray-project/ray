import sys
import json
from typing import List

import pytest

from ci.ray_ci.automation.determine_microcheck_tests import (
    _get_failed_prs,
    _get_test_with_minimal_coverage,
    _update_high_impact_tests,
)
from ci.ray_ci.utils import ci_init
from ray_release.result import ResultStatus
from ray_release.test import TestResult, Test

ci_init()

DB = {}


class MockTest(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_name(self) -> str:
        return self.get("name", "")

    def get_test_results(self, limit: int, aws_bucket: str) -> List[TestResult]:
        return self.get("test_results", [])

    def persist_to_s3(self) -> None:
        DB[self["name"]] = json.dumps(self)


def stub_test_result(status: ResultStatus, branch: str) -> TestResult:
    return TestResult(
        status=status.value,
        branch=branch,
        commit="",
        url="",
        timestamp=0,
        pull_request="",
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


def test_get_failed_prs():
    assert _get_failed_prs(
        MockTest(
            {
                "name": "test",
                "test_results": [
                    stub_test_result(ResultStatus.ERROR, "w00t"),
                    stub_test_result(ResultStatus.ERROR, "w00t"),
                    stub_test_result(ResultStatus.SUCCESS, "hi"),
                    stub_test_result(ResultStatus.ERROR, "f00"),
                ],
            }
        ),
        1,
    ) == {"w00t", "f00"}


def test_get_test_with_minimal_coverage():
    # empty cases
    assert _get_test_with_minimal_coverage({}, 50) == set()

    # normal cases
    test_to_prs = {
        "test1": {"a"},
        "test2": {"a", "b"},
        "test3": {"c"},
        "test4": {"d"},
    }
    assert _get_test_with_minimal_coverage(test_to_prs, 0) == set()
    assert _get_test_with_minimal_coverage(test_to_prs, 50) == {"test2"}
    assert _get_test_with_minimal_coverage(test_to_prs, 75) == {
        "test2",
        "test3",
    }

    # one beat all cases
    test_to_prs = {
        "test1": {"a"},
        "test2": {"a", "b"},
        "test3": {"a", "b", "c"},
    }
    assert _get_test_with_minimal_coverage(test_to_prs, 50) == {"test3"}
    assert _get_test_with_minimal_coverage(test_to_prs, 75) == {"test3"}

    # equal distribution cases
    test_to_prs = {
        "test1": {"a"},
        "test2": {"b"},
        "test3": {"c"},
    }
    assert _get_test_with_minimal_coverage(test_to_prs, 100) == {
        "test1",
        "test2",
        "test3",
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
