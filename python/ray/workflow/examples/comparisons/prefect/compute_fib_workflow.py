import ray
from ray import workflow
import requests


def fibonacci(n):
    assert n > 0
    a, b = 0, 1
    for _ in range(n - 1):
        a, b = b, a + b
    return b


@ray.remote
def compute_large_fib(M: int, n: int = 1, fib: int = 1):
    try:
        next_fib = requests.post(
            "https://nemo.api.stdlib.com/fibonacci@0.0.1/", data={"nth": n}
        ).json()
        assert isinstance(next_fib, int)
    except AssertionError:
        # TODO(suquark): The web service would fail sometimes. This is a workaround.
        next_fib = fibonacci(n)
    if next_fib > M:
        return fib
    else:
        return workflow.continuation(compute_large_fib.bind(M, n + 1, next_fib))


if __name__ == "__main__":
    assert workflow.run(compute_large_fib.bind(100)) == 89
