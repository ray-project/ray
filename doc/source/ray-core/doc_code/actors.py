# flake8: noqa
# __actor_start__
import ray

@ray.remote
class Counter(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def get_counter(self):
        return self.value

# Create an actor from this class.
counter = Counter.remote()
# __actor_end__

# __resource_start__
# Specify required resources for an actor.
@ray.remote(num_cpus=2, num_gpus=0.5)
class Actor(object):
    pass
# __resource_end__

# __call_start__
# Call the actor.
obj_ref = counter.increment.remote()
assert ray.get(obj_ref) == 1
# __call_end__


# __multi_call_start__
# Create ten Counter actors.
counters = [Counter.remote() for _ in range(10)]

# Increment each Counter once and get the results. These tasks all happen in
# parallel.
results = ray.get([c.increment.remote() for c in counters])
print(results)  # prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

# Increment the first Counter five times. These tasks are executed serially
# and share state.
results = ray.get([counters[0].increment.remote() for _ in range(5)])
print(results)  # prints [2, 3, 4, 5, 6]
# __multi_call_end__


# __pass_handle_f_start__
import time

@ray.remote
def f(counter):
    for _ in range(1000):
        time.sleep(0.1)
        counter.increment.remote()
# __pass_handle_f_end__


# __pass_handle_start__
counter = Counter.remote()

# Start some tasks that use the actor.
[f.remote(counter) for _ in range(3)]

# Print the counter value.
for _ in range(10):
    time.sleep(1)
    print(ray.get(counter.get_counter.remote()))
# __pass_handle_end__
