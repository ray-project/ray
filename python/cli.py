"""Wrapper for Ray CLI that allows us to run it from inside custom Python environment."""
import ray
from ray.scripts.scripts import main

import time

def test():
    ray.init()

    @ray.remote(num_accs=1)
    class ACCActor:
        def say_hello(self):
            print("I live in a pod with ACC access.")
            time.sleep(60)

    # Request actor placement.
    acc_actors = [ACCActor.remote() for _ in range(1)]
    # The following command will block until two Ray pods with ACC access are scaled
    # up and the actors are placed.
    ray.get([actor.say_hello.remote() for actor in acc_actors])


if __name__ == "__main__":
    # main()
    test()
