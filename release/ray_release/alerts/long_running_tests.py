from typing import Optional

from ray_release.config import Test
from ray_release.result import Result


def handle_result(
    test: Test,
    result: Result,
) -> Optional[str]:
    last_update_diff = result.results.get("last_update_diff", float("inf"))

    test_name = test["legacy"]["test_name"]

    if test_name in [
        "actor_deaths",
        "many_actor_tasks",
        "many_drivers",
        "many_tasks",
        "many_tasks_serialized_ids",
        "node_failures",
        "object_spilling_shuffle",
    ]:
        # Core tests
        target_update_diff = 300

    elif test_name in ["apex", "impala", "many_ppo", "pbt"]:
        # Tune/RLlib style tests
        target_update_diff = 480
    elif test_name in ["serve", "serve_failure"]:
        # Serve tests have workload logs every five minutes.
        # Leave up to 180 seconds overhead.
        target_update_diff = 480
    else:
        return None

    if last_update_diff > target_update_diff:
        return (
            f"Last update to results json was too long ago "
            f"({last_update_diff:.2f} > {target_update_diff})"
        )

    return None
