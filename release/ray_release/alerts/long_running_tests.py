from typing import Optional

from ray_release.result import Result
from ray_release.test import Test


def handle_result(
    test: Test,
    result: Result,
) -> Optional[str]:
    last_update_diff = result.results.get("last_update_diff", float("inf"))

    test_name = test["name"]

    if test_name in [
        "long_running_actor_deaths",
        "long_running_many_actor_tasks",
        "long_running_many_drivers",
        "long_running_many_tasks",
        "long_running_many_tasks_serialized_ids",
        "long_running_node_failures",
    ]:
        # Core tests
        target_update_diff = 300

    elif test_name in [
        "long_running_apex",
        "long_running_impala",
        "long_running_many_ppo",
    ]:
        # Tune/RLlib style tests
        target_update_diff = 480
    elif test_name in ["long_running_serve"]:
        # Serve tests have workload logs every five minutes.
        # Leave up to 180 seconds overhead.
        target_update_diff = 480
    elif test_name in ["long_running_serve_failure"]:
        # TODO (shrekris-anyscale): set update_diff limit for serve failure
        target_update_diff = float("inf")
    else:
        return None

    if last_update_diff > target_update_diff:
        return (
            f"Last update to results json was too long ago "
            f"({last_update_diff:.2f} > {target_update_diff})"
        )

    return None
