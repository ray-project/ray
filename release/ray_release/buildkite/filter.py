import copy
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from ray_release.buildkite.settings import Frequency, get_frequency
from ray_release.configs.global_config import get_global_config
from ray_release.test import Test
from ray_release.test_automation.state_machine import TestStateMachine


def _unflattened_lookup(lookup: Dict, flat_key: str, delimiter: str = "/") -> Any:
    curr = lookup
    for k in flat_key.split(delimiter):
        try:
            curr = curr.get(k, {})
        except Exception:
            return None
    return curr


def filter_tests(
    test_collection: List[Test],
    frequency: Frequency,
    test_filters: Optional[Dict[str, list]] = None,
    prefer_smoke_tests: bool = False,
    run_jailed_tests: bool = False,
    run_unstable_tests: bool = False,
) -> List[Tuple[Test, bool]]:
    if test_filters is None:
        test_filters = {}

    tests_to_run = []
    for test in test_collection:
        attr_mismatch = False
        # Skip kuberay tests for now.
        # TODO: (khluu) Remove this once we start running KubeRay release tests.
        if test.is_kuberay() and get_global_config()["kuberay_disabled"]:
            continue

        # Check if test attributes match filters
        # Logic: OR within same attribute, AND across different attributes
        if test_filters:
            for attr, values in test_filters.items():
                # Check if at least one value matches for this attribute (OR logic)
                attr_matched = False
                for value in values:
                    # Only prefix filter doesn't use regex
                    if attr == "prefix":
                        if test.get_name().startswith(value):
                            attr_matched = True
                            break
                    else:  # Match filters using regex
                        attr_value = _unflattened_lookup(test, attr) or ""
                        if re.match(value, attr_value):
                            attr_matched = True
                            break

                # If none of the values matched for this attribute, skip this test
                if not attr_matched:
                    attr_mismatch = True
                    break
        if attr_mismatch:
            continue

        if not run_jailed_tests:
            clone_test = copy.deepcopy(test)
            clone_test.update_from_s3()
            if clone_test.is_jailed_with_open_issue(TestStateMachine.get_ray_repo()):
                continue
        if not run_unstable_tests:
            if not test.get("stable", True):
                continue

        test_frequency = get_frequency(test["frequency"])

        if frequency == Frequency.ANY or frequency == test_frequency:
            if prefer_smoke_tests and "smoke_test" in test:
                # If we prefer smoke tests and a smoke test is available for this test,
                # then use the smoke test
                smoke_test = True
            else:
                smoke_test = False
            tests_to_run.append((test, smoke_test))
            continue

        elif "smoke_test" in test:
            smoke_frequency = get_frequency(test["smoke_test"]["frequency"])
            if smoke_frequency == frequency:
                tests_to_run.append((test, True))
    return tests_to_run


def group_tests(
    test_collection_filtered: List[Tuple[Test, bool]]
) -> Dict[str, List[Tuple[Test, bool]]]:
    groups = defaultdict(list)
    for test, smoke in test_collection_filtered:
        group = test.get("group", "Ungrouped release tests")
        groups[group].append((test, smoke))
    return groups
