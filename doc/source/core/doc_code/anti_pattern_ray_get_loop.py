# __anti_pattern_start__
import ray

ray.init()


@ray.remote
def f(i):
    return i


# Anti-pattern: no parallelism due to calling ray.get inside of the loop.
sequential_returns = []
for i in range(100):
    sequential_returns.append(ray.get(f.remote(i)))

# Better approach: parallelism because the tasks are executed in parallel.
refs = []
for i in range(100):
    refs.append(f.remote(i))

parallel_returns = ray.get(refs)
# __anti_pattern_end__

assert sequential_returns == parallel_returns
