import ray
import os
import time
import signal
import sys
import pytest

from ray.job_config import JobConfig

from ray._private.test_utils import (
    wait_for_condition,
    wait_for_pid_to_exit,
)

from ray.exceptions import RayActorError

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


@pytest.mark.parametrize("default_actor_lifetime", ["detached", "non_detached"])
@pytest.mark.parametrize("child_actor_lifetime", [None, "detached", "non_detached"])
def test_default_actor_lifetime(default_actor_lifetime, child_actor_lifetime):
    @ray.remote
    class OwnerActor:
        def create_child_actor(self, child_actor_lifetime):
            if child_actor_lifetime is None:
                self._child_actor = ChildActor.remote()
            else:
                self._child_actor = ChildActor.options(
                    lifetime=child_actor_lifetime
                ).remote()
            assert "ok" == ray.get(self._child_actor.ready.remote())
            return self._child_actor

        def get_pid(self):
            return os.getpid()

        def ready(self):
            return "ok"

    @ray.remote
    class ChildActor:
        def ready(self):
            return "ok"

    if default_actor_lifetime is not None:
        ray.init(job_config=JobConfig(default_actor_lifetime=default_actor_lifetime))
    else:
        ray.init()

    # 1. create owner and invoke create_child_actor.
    owner = OwnerActor.remote()
    child = ray.get(owner.create_child_actor.remote(child_actor_lifetime))
    assert "ok" == ray.get(child.ready.remote())
    # 2. Kill owner and make sure it's dead.
    owner_pid = ray.get(owner.get_pid.remote())
    os.kill(owner_pid, SIGKILL)
    wait_for_pid_to_exit(owner_pid)

    # 3. Assert child state.

    def is_child_actor_dead():
        try:
            ray.get(child.ready.remote())
            return False
        except RayActorError:
            return True

    actual_lifetime = default_actor_lifetime
    if child_actor_lifetime is not None:
        actual_lifetime = child_actor_lifetime

    assert actual_lifetime is not None
    if actual_lifetime == "detached":
        time.sleep(5)
        assert not is_child_actor_dead()
    else:
        wait_for_condition(is_child_actor_dead, timeout=5)

    ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
