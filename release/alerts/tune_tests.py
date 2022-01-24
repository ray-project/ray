import datetime

from typing import Dict, Optional


def handle_result(created_on: datetime.datetime, category: str,
                  test_suite: str, test_name: str, status: str, results: Dict,
                  artifacts: Dict, last_logs: str, team: str) -> Optional[str]:
    assert test_suite == "tune_tests"

    msg = ""
    success = status == "finished"
    time_taken = results.get("time_taken", float("inf"))
    num_terminated = results.get("trial_states", {}).get("TERMINATED", 0)
    was_smoke_test = results.get("smoke_test", False)

    if not success:
        if status == "timeout":
            msg += "Test timed out."
        else:
            msg += "Test script failed. "

    if test_name == "long_running_large_checkpoints":
        last_update_diff = results.get("last_update_diff", float("inf"))
        target_update_diff = 360

        if last_update_diff > target_update_diff:
            return f"Last update to results json was too long ago " \
                   f"({last_update_diff:.2f} > {target_update_diff})"
        return None

    elif test_name == "bookkeeping_overhead":
        target_terminated = 10000
        target_time = 800
    elif test_name == "durable_trainable":
        target_terminated = 16
        target_time = 600
    elif test_name == "network_overhead":
        target_terminated = 100 if not was_smoke_test else 20
        target_time = 900 if not was_smoke_test else 400
    elif test_name == "result_throughput_cluster":
        target_terminated = 1000
        target_time = 120
    elif test_name == "result_throughput_single_node":
        target_terminated = 96
        target_time = 120
    elif test_name == "xgboost_sweep":
        target_terminated = 31
        target_time = 3600
    else:
        return None

    if num_terminated < target_terminated:
        msg += f"Some trials failed " \
               f"(num_terminated={num_terminated} < {target_terminated}). "
    if time_taken > target_time:
        msg += f"Took too long to complete " \
               f"(time_taken={time_taken:.2f} > {target_time}). "

    return msg or None
