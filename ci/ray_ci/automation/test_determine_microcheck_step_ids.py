import sys
from typing import List

import pytest

from ci.ray_ci.automation.determine_microcheck_step_ids import (
    _get_high_impact_step_ids,
)
from ray_release.result import ResultStatus
from ray_release.test import TestResult, Test


class MockTest(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_name(self) -> str:
        return self.get("name", "")

    def get_test_results(self, limit: int) -> List[TestResult]:
        return self.get("test_results", [])

    def is_high_impact(self) -> bool:
        return self.get(Test.KEY_IS_HIGH_IMPACT, "false") == "true"


def stub_test_result(rayci_step_id: str) -> TestResult:
    return TestResult(
        status=ResultStatus.SUCCESS.value,
        branch="master",
        commit="",
        url="",
        timestamp=0,
        pull_request="",
        rayci_step_id=rayci_step_id,
    )


def test_get_high_impact_test_ids():
    tests = [
        MockTest(
            {
                "name": "core_test",
                Test.KEY_IS_HIGH_IMPACT: "false",
                "test_results": [
                    stub_test_result("corebuild"),
                ],
            }
        ),
        MockTest(
            {
                "name": "data_test",
                Test.KEY_IS_HIGH_IMPACT: "true",
                "test_results": [
                    stub_test_result("databuild"),
                ],
            }
        ),
    ]

    assert _get_high_impact_step_ids(tests) == ["databuild"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
