# flake8: noqa

# fmt: off
# __actor_restart_begin__
import os
import ray

ray.init()

# This actor kills itself after executing 10 tasks.
@ray.remote(max_restarts=4, max_task_retries=-1)
class Actor:
    def __init__(self):
        self.counter = 0

    def increment_and_possibly_fail(self):
        # Exit after every 10 tasks.
        if self.counter == 10:
            os._exit(0)
        self.counter += 1
        return self.counter

actor = Actor.remote()

# The actor will be reconstructed up to 4 times, so we can execute up to 50
# tasks successfully. The actor is reconstructed by rerunning its constructor.
# Methods that were executing when the actor died will be retried and will not
# raise a `RayActorError`. Retried methods may execute twice, once on the
# failed actor and a second time on the restarted actor.
for _ in range(50):
    counter = ray.get(actor.increment_and_possibly_fail.remote())
    print(counter)  # Prints the sequence 1-10 5 times.

# After the actor has been restarted 4 times, all subsequent methods will
# raise a `RayActorError`.
for _ in range(10):
    try:
        counter = ray.get(actor.increment_and_possibly_fail.remote())
        print(counter)  # Unreachable.
    except ray.exceptions.RayActorError:
        print("FAILURE")  # Prints 10 times.

# __actor_restart_end__
# fmt: on
