from typing import Any

from ray import workflow


@workflow.step
def flaky_step() -> str:
    import random

    if random.choice([0, 1, 1]) != 0:
        raise ValueError("oops")
    
    return "ok"


@workflow.step
def custom_retry_strategy(func: Any, num_retries: int, delay_s: int) -> str:
    import time

    def handle_result(res: Tuple[str, Exception]) -> str
        result, error = res
        if result:
            return res
        elif num_retries <= 0:
            raise error
        else:
            print("Retrying exception after delay", error)
            time.sleep(delay_s)
            return retry_flaky_step(func, num_retries - 1, delay_s)

    res = func.options(catch_exceptions=True).step()
    return handle_result.step(res)


if __name__ == "__main__":
    workflow.init()

    # Default retry strategy.
    print(flaky_step.options(max_retries=10).run())

    # Custom strategy.
    print(custom_retry_strategy.step(flaky_step).run())
