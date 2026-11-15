import sys

if len(sys.argv) == 1:
    # A small number for CI test.
    sys.argv.append("5")

# __program_start__
import sys
import ray

# fmt: off
# __large_values_start__
import numpy as np


@ray.remote
def large_values(num_returns):
    return [
        np.random.randint(np.iinfo(np.int8).max, size=(100_000_000, 1), dtype=np.int8)
        for _ in range(num_returns)
    ]
# __large_values_end__
# fmt: on


# fmt: off
# __large_values_generator_start__
@ray.remote
def large_values_generator(num_returns):
    for i in range(num_returns):
        yield np.random.randint(
            np.iinfo(np.int8).max, size=(100_000_000, 1), dtype=np.int8
        )
        print(f"yielded return value {i}")
# __large_values_generator_end__
# fmt: on


# A large enough value (e.g. 100).
num_returns = int(sys.argv[1])
# Worker will likely OOM using normal returns.
print("Using normal functions...")
try:
    ray.get(
        large_values.options(num_returns=num_returns, max_retries=0).remote(
            num_returns
        )[0]
    )
except ray.exceptions.WorkerCrashedError:
    print("Worker failed with normal function")

# Using a generator will allow the worker to finish.
# Note that this will block until the full task is complete, i.e. the
# last yield finishes.
print("Using generators...")
ray.get(
    large_values_generator.options(num_returns=num_returns, max_retries=0).remote(
        num_returns
    )[0]
)
print("Success!")
