import re
from collections import defaultdict
from typing import List, Optional, Tuple, Dict, Any

from ray_release.buildkite.settings import Frequency, get_frequency
from ray_release.config import Test


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
    test_attr_regex_filters: Optional[Dict[str, str]] = None,
    prefer_smoke_tests: bool = False,
) -> List[Tuple[Test, bool]]:
    if test_attr_regex_filters is None:
        test_attr_regex_filters = {}

    tests_to_run = []
    for test in test_collection:
        # First, filter by string attributes
        attr_mismatch = False
        for attr, regex in test_attr_regex_filters.items():
            if not re.fullmatch(regex, _unflattened_lookup(test, attr) or ""):
                attr_mismatch = True
                break
        if attr_mismatch:
            continue

        test_frequency = get_frequency(test["frequency"])
        if test_frequency == Frequency.DISABLED:
            # Skip disabled tests
            continue

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
