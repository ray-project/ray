from typing import Optional

from ray_release.config import Test
from ray_release.result import Result


def handle_result(
    test: Test,
    result: Result,
) -> Optional[str]:

    if result.status != "finished":
        return f"Test script did not finish successfully ({result.status})."

    return None
