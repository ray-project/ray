import pytest
import sys
import time

import ray
from ray.test_utils import run_string_as_driver


def test_isolation(shutdown_only):
    job_config = ray.job_config.JobConfig(namespace="namespace")
    info = ray.init(job_config=job_config)

    address = info["redis_address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

job_config = ray.job_config.JobConfig(namespace="{}")
ray.init(address="{}", job_config=job_config)

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
    """

    # Start a detached actor in a different namespace.
    run_string_as_driver(driver_template.format("different", address))

    @ray.remote
    class Actor:
        def ping(self):
            return "pong"

    # Create an actor. This should succeed because the other actor is in a
    # different namespace.
    probe = Actor.options(name="Pinger").remote()
    assert ray.get(probe.ping.remote()) == "pong"
    del probe

    # Wait for actor removal
    actor_removed = False
    for _ in range(50):  # Timeout after 5s
        try:
            ray.get_actor("Pinger")
        except ValueError:
            actor_removed = True
            # This means the actor was removed.
            break
        else:
            time.sleep(0.1)

    assert actor_removed, "This is an anti-flakey test measure"

    try:
        ray.get_actor("Pinger")
    except ValueError:
        pass
    else:
        assert False, "Something went wrong, the prob actor wasn't cleaned up."

    # Now make the actor in this namespace, from a different job.
    run_string_as_driver(driver_template.format("namespace", address))

    detached_actor = ray.get_actor("Pinger")
    assert ray.get(detached_actor.ping.remote()) == "pong from other job"

    try:
        probe = Actor.options(name="Pinger", lifetime="detached").remote()
        ray.get(probe.ping.remote())
    except ValueError:
        pass
    else:
        assert False, "We were allowed to create multiple actors with the "\
                      "the name, in the same namespace"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
