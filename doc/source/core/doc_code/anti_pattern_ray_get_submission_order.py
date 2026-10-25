# __anti_pattern_start__
import random
import time
import ray

ray.init()


@ray.remote
def f(i):
    time.sleep(random.random())
    return i


# Anti-pattern: process results in the submission order.
sum_in_submission_order = 0
refs = [f.remote(i) for i in range(100)]
for ref in refs:
    # Blocks until this ObjectRef is ready.
    result = ray.get(ref)
    # process result
    sum_in_submission_order = sum_in_submission_order + result

# Better approach: process results in the completion order.
sum_in_completion_order = 0
refs = [f.remote(i) for i in range(100)]
unfinished = refs
while unfinished:
    # Returns the first ObjectRef that is ready.
    finished, unfinished = ray.wait(unfinished, num_returns=1)
    result = ray.get(finished[0])
    # process result
    sum_in_completion_order = sum_in_completion_order + result
# __anti_pattern_end__

assert sum_in_submission_order == sum_in_completion_order
