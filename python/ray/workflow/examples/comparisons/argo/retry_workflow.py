from typing import Any, Tuple, Optional

import ray
from ray import workflow


@ray.remote
def flaky_step() -> str:
    import random

    if random.choice([0, 1, 1]) != 0:
        raise ValueError("oops")

    return "ok"


@ray.remote
def custom_retry_strategy(func: Any, num_retries: int, delay_s: int) -> str:
    import time

    @ray.remote
    def handle_result(res: Tuple[Optional[str], Optional[Exception]]) -> str:
        result, error = res
        if result:
            return res
        elif num_retries <= 0:
            raise error
        else:
            print("Retrying exception after delay", error)
            time.sleep(delay_s)
            return workflow.continuation(
                custom_retry_strategy.bind(func, num_retries - 1, delay_s)
            )

    res = func.options(**workflow.options(catch_exceptions=True)).bind()
    return workflow.continuation(handle_result.bind(res))


if __name__ == "__main__":
    workflow.init()
    # Default retry strategy.
    print(
        workflow.create(
            flaky_step.options(**workflow.options(max_retries=10)).bind()
        ).run()
    )
    # Custom strategy.
    print(workflow.create(custom_retry_strategy.bind(flaky_step, 10, 1)).run())
