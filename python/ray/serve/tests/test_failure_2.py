import sys

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor


def test_no_available_replicas_does_not_block_proxy(serve_instance):
    """Test that handle blocking waiting for replicas doesn't block proxy.

    This is essential so that other requests and health checks can pass while a
    deployment is deploying/updating.

    See https://github.com/ray-project/ray/issues/36460.
    """

    @serve.deployment
    class SlowStarter:
        def __init__(self, starting_actor, finish_starting_actor):
            ray.get(starting_actor.send.remote())
            ray.get(finish_starting_actor.wait.remote())

        def __call__(self):
            return "hi"

    @ray.remote
    def make_blocked_request():
        r = httpx.get("http://localhost:8000/")
        r.raise_for_status()
        return r.text

    # Loop twice: first iteration tests deploying from nothing, second iteration
    # tests updating the replicas of an existing deployment.
    for _ in range(2):
        starting_actor = SignalActor.remote()
        finish_starting_actor = SignalActor.remote()
        serve._run(
            SlowStarter.bind(starting_actor, finish_starting_actor), _blocking=False
        )

        # Ensure that the replica has been started (we use _blocking=False).
        ray.get(starting_actor.wait.remote())

        # The request shouldn't complete until the replica has finished started.
        blocked_ref = make_blocked_request.remote()
        with pytest.raises(TimeoutError):
            ray.get(blocked_ref, timeout=1)
        # If the proxy's loop was blocked, these would hang.
        httpx.get("http://localhost:8000/-/routes").raise_for_status()
        httpx.get("http://localhost:8000/-/healthz").raise_for_status()

        # Signal the replica to finish starting; request should complete.
        ray.get(finish_starting_actor.send.remote())
        assert ray.get(blocked_ref) == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
