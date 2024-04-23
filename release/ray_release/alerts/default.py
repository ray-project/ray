from typing import Optional

from ray_release.test import Test
from ray_release.result import Result, ResultStatus


def handle_result(
    test: Test,
    result: Result,
) -> Optional[str]:

    if result.status != ResultStatus.SUCCESS.value:
        return f"Test script did not finish successfully ({result.status})."

    return None
