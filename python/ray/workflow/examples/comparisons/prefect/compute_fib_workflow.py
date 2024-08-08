import tempfile

import ray
from ray import workflow
from ray.actor import ActorHandle


@ray.remote
class FibonacciActor:
    def __init__(self):
        self.cache = {}

    def compute(self, n):
        if n not in self.cache:
            assert n > 0
            a, b = 0, 1
            for _ in range(n - 1):
                a, b = b, a + b
            self.cache[n] = b
        return self.cache[n]


@ray.remote
def compute_large_fib(fibonacci_actor: ActorHandle, M: int, n: int = 1, fib: int = 1):
    next_fib = ray.get(fibonacci_actor.compute.remote(n))
    if next_fib > M:
        return fib
    else:
        return workflow.continuation(
            compute_large_fib.bind(fibonacci_actor, M, n + 1, next_fib)
        )


if __name__ == "__main__":
    ray.init(storage=f"file://{tempfile.TemporaryDirectory().name}")
    assert workflow.run(compute_large_fib.bind(FibonacciActor.remote(), 100)) == 89
