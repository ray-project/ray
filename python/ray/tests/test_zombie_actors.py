import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import time
import os
import ray
import signal
import numpy as np


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(100)
def test_zombie_actors(ray_start_10_cpus):
    # Actor class that creates nested actors for a specified depth
    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self):
            self.child = None
            pass

        def get_pid(self):
            return os.getpid()

        def extend_branch(self, depth):
            if depth > 0:
                self.child = Actor.remote()
                ray.get(self.child.extend_branch.remote(depth - 1))
            return "OK"

    def kill_actor(actor):
        """A helper function that kills an actor process."""
        pid = ray.get(actor.get_pid.remote())
        os.kill(pid, signal.SIGKILL)
        time.sleep(1)

    for i in range(5):
        # Spawn an independent actor tree with some depth.
        # Then kill the root actor, leaving zombie actors.
        # The zombie actors should be cleaned up by the system
        # or else the resources would run out and the test would hang.
        try:
            root_actor = Actor.remote()
            ray.get(root_actor.extend_branch.remote(depth=5))
            kill_actor(root_actor)
        except Exception:
            assert False
