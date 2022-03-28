import re
from collections import defaultdict
from typing import List, Optional, Tuple, Dict

from ray_release.buildkite.settings import Frequency, get_frequency
from ray_release.config import Test


def filter_tests(
    test_collection: List[Test],
    frequency: Frequency,
    test_name_filter: Optional[str] = None,
) -> List[Tuple[Test, bool]]:
    tests_to_run = []
    for test in test_collection:
        # First, filter by name
        if test_name_filter and not re.match(test_name_filter, test["name"]):
            continue

        test_frequency = get_frequency(test["frequency"])
        if test_frequency == Frequency.DISABLED:
            # Skip disabled tests
            continue

        if frequency == Frequency.ANY or frequency == test_frequency:
            tests_to_run.append((test, False))
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
