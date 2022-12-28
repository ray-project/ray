from typing import Optional

from ray_release.config import Test
from ray_release.result import Result


def handle_result(
    test: Test,
    result: Result,
) -> Optional[str]:
    test_name = test["legacy"]["test_name"]

    msg = ""
    success = result.status == "finished"
    time_taken = result.results.get("time_taken", float("inf"))
    num_terminated = result.results.get("trial_states", {}).get("TERMINATED", 0)
    was_smoke_test = result.results.get("smoke_test", False)

    if not success:
        if result.status == "timeout":
            msg += "Test timed out."
        else:
            msg += "Test script failed. "

    if test_name == "long_running_large_checkpoints":
        last_update_diff = result.results.get("last_update_diff", float("inf"))
        target_update_diff = 360

        if last_update_diff > target_update_diff:
            return (
                f"Last update to results json was too long ago "
                f"({last_update_diff:.2f} > {target_update_diff})"
            )
        return None

    elif test_name == "bookkeeping_overhead":
        target_terminated = 10000
        target_time = 800
    elif test_name == "durable_trainable":
        target_terminated = 16
        target_time = 650
    elif test_name == "network_overhead":
        target_terminated = 100 if not was_smoke_test else 20
        target_time = 900 if not was_smoke_test else 400
    elif test_name == "result_throughput_cluster":
        target_terminated = 1000
        target_time = 135
    elif test_name == "result_throughput_single_node":
        target_terminated = 96
        target_time = 120
    elif test_name == "xgboost_sweep":
        target_terminated = 31
        target_time = 3600
    else:
        return None

    if num_terminated < target_terminated:
        msg += (
            f"Some trials failed "
            f"(num_terminated={num_terminated} < {target_terminated}). "
        )
    if time_taken > target_time:
        msg += (
            f"Took too long to complete "
            f"(time_taken={time_taken:.2f} > {target_time}). "
        )

    return msg or None
