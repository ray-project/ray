import sys

import pytest

import ray
from ray.util import ActorPool


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="This test is specifically for a Windows access violation bug",
)
def test_actorpool_windows_teardown_crash():
    """
    Tests that initializing an ActorPool with a runtime_env on Windows
    does not cause a fatal access violation during Ray shutdown teardown.
    See: https://github.com/ray-project/ray/issues/62442
    """
    # Initialize ray with a dummy runtime_env that triggers the setup hook
    ray.init(runtime_env={"worker_process_setup_hook": lambda: None})

    @ray.remote
    class DummyActor:
        def do_work(self):
            return "success"

    try:
        # Create an ActorPool
        actors = [DummyActor.remote() for _ in range(2)]
        pool = ActorPool(actors)

        # Submit dummy work
        results = list(pool.map(lambda a, v: a.do_work.remote(), [1, 2]))
        assert results == ["success", "success"]
    finally:
        # The segfault would typically occur during ray.shutdown() due to C++ destructors
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
