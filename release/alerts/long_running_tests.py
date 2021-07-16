import datetime

from typing import Dict, Optional


def handle_result(created_on: datetime.datetime, category: str,
                  test_suite: str, test_name: str, status: str, results: Dict,
                  artifacts: Dict, last_logs: str) -> Optional[str]:
    assert test_suite == "long_running_tests"

    # elapsed_time = results.get("elapsed_time", 0.)
    last_update_diff = results.get("last_update_diff", float("inf"))

    if test_name in [
            "actor_deaths", "many_actor_tasks", "many_drivers", "many_tasks",
            "many_tasks_serialized_ids", "node_failures",
            "object_spilling_shuffle", "serve", "serve_failure"
    ]:
        # Core tests
        target_update_diff = 120

    elif test_name in ["apex", "impala", "many_ppo", "pbt"]:
        # Tune/RLLib style tests
        target_update_diff = 360
    else:
        return None

    if last_update_diff > target_update_diff:
        return f"Last update to results json was too long ago " \
               f"({last_update_diff:.2f} > {target_update_diff})"

    return None
