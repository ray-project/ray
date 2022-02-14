import datetime

from typing import Dict, Optional


def handle_result(
    created_on: datetime.datetime,
    category: str,
    test_suite: str,
    test_name: str,
    status: str,
    results: Dict,
    artifacts: Dict,
    last_logs: str,
    team: str,
) -> Optional[str]:
    assert test_suite == "xgboost_tests"

    time_taken = results.get("time_taken", float("inf"))
    num_terminated = results.get("trial_states", {}).get("TERMINATED", 0)

    if test_name in ["distributed_api_test", "ft_small_elastic", "ft_small_nonelastic"]:
        if not status == "finished":
            return f"Test script did not finish successfully ({status})."

        return None
    elif test_name.startswith("tune_"):
        msg = ""
        if test_name == "tune_small":
            target_terminated = 4
            target_time = 90
        elif test_name == "tune_4x32":
            target_terminated = 4
            target_time = 120
        elif test_name == "tune_32x4":
            target_terminated = 32
            target_time = 600
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
                f"(time_taken={time_taken} > {target_time}). "
            )

        return msg or None
    else:
        # train scripts
        if test_name == "train_small":
            # Leave a couple of seconds for ray connect setup
            # (without connect it should finish in < 30)
            target_time = 45
        elif test_name == "train_moderate":
            target_time = 60
        elif test_name == "train_gpu":
            target_time = 40
        else:
            return None

        if time_taken > target_time:
            return (
                f"Took too long to complete "
                f"(time_taken={time_taken:.2f} > {target_time}). "
            )

    return None
