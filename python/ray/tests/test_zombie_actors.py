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
def test_zombie_actors(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 10
    for i in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(redis_address=cluster.redis_address)

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


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(100)
def test_child_parent_actor_race(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 5
    for i in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self, sleep_time=0):
            self.child = None
            time.sleep(sleep_time)
            pass

        def get_pid(self):
            return os.getpid()

        def create_child(self, depth):
            if depth == 1:
                # Delay the child creation process by sleeping in the constructor.
                self.child = Actor.remote(5)
            else:
                self.child = Actor.remote()
                ray.get(self.child.create_child.remote(depth - 1))
            return "OK"

    def kill_actor(actor):
        """A helper function that kills an actor process."""
        pid = ray.get(actor.get_pid.remote())
        os.kill(pid, signal.SIGKILL)
        time.sleep(1)

    for i in range(10):
        # Parent actor spawns child actors and dies immediately after.
        # The system should correctly kill the child actor if the parent
        # actor is dead, accounting for the corner case when the parent
        # dies before the child is fully created.
        try:
            parent_actor = Actor.remote()
            ray.get(parent_actor.create_child.remote(num_nodes - 1))
            kill_actor(parent_actor)
        except Exception:
            assert False
