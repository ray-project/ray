from ray import workflow
import requests


@workflow.step
def compute_large_fib(M: int, n: int = 1, fib: int = 1):
    next_fib = requests.post(
        "https://nemo.api.stdlib.com/fibonacci@0.0.1/", data={"nth": n}
    ).json()
    if next_fib > M:
        return fib
    else:
        return compute_large_fib.step(M, n + 1, next_fib)


if __name__ == "__main__":
    workflow.init()
    assert compute_large_fib.step(100).run() == 89
