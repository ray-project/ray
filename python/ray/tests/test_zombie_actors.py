import pytest
import time
import os

import ray
import numpy as np

def test_zombie_actors(ray_start_regular):
    #@ray.remote(num_cpus=1)
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def extend_branch(self, depth):
            a = Actor.remote()
            if depth > 0:
                ray.get(a.extend_branch.remote(depth-1))
            return np.zeros(1000)

    def kill_actor(actor):
        """A helper function that kills an actor process."""
        pid = ray.get(actor.get_pid.remote())
        os.kill(pid, signal.SIGKILL)
        time.sleep(1)

    for i in range(5):
        try:
            parent_actor = Actor.remote()
            ray.get(parent_actor.extend_branch.remote(depth=5))
            #now kill parent_actor
            #!TODO: this should be uncommented
            #kill_actor(parent_actor)
        except Exception as e:
            assert False
            
