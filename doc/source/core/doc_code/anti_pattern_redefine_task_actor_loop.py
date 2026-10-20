# __anti_pattern_start__
import ray

ray.init()

outputs = []
for i in range(10):

    @ray.remote
    def double(i):
        return i * 2

    outputs.append(double.remote(i))
outputs = ray.get(outputs)
# The double remote function is pickled and uploaded 10 times.
# __anti_pattern_end__

assert outputs == [i * 2 for i in range(10)]


# __better_approach_start__
@ray.remote
def double(i):
    return i * 2


outputs = []
for i in range(10):
    outputs.append(double.remote(i))
outputs = ray.get(outputs)
# The double remote function is pickled and uploaded 1 time.
# __better_approach_end__

assert outputs == [i * 2 for i in range(10)]
