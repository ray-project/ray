import ray
import os
import time
import signal
import sys
import pytest

from ray._private.test_utils import (
    wait_for_condition,
    wait_for_pid_to_exit,
)

from ray.exceptions import RayActorError

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM

@pytest.mark.parametrize(
    "default_actor_lifetime,child_actor_lifetime",
    [(None, None), (None, "detached"), (None, "non-detached"),
     ("detached", None), ("detached", "detached"),
     ("detached", "non-detached"), ("non-detached", None),
     ("non-detached", "detached"), ("non-detached", "non-detached")])
def test_default_actor_lifetime(default_actor_lifetime, child_actor_lifetime):
    @ray.remote
    class OwnerActor:
        def create_child_actor(self, child_actor_lifetime):
            if child_actor_lifetime is None:
                self._child_actor = ChildActor.remote()
            else:
                self._child_actor = ChildActor.options(
                    lifetime=child_actor_lifetime).remote()
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
        ray.init(_default_actor_lifetime=default_actor_lifetime)
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

    def _assert_child_actor_still_alive():
        time.sleep(4)
        assert "ok" == ray.get(child.ready.remote())

    def _assert_child_actor_is_dead():
        def is_dead():
            try:
                ray.get(child.ready.remote())
                return False
            except RayActorError:
                return True
        wait_for_condition(is_dead, timeout=5)

    if child_actor_lifetime is not None:
        # child_actor_lifetime is specifying at runtime.
        if child_actor_lifetime == "detached":
            _assert_child_actor_still_alive()
        else:
            _assert_child_actor_is_dead()
    else:
        # Code path of not specifying child_actor_lifetime, so it's
        # depends on the default actor lifetime.
        if default_actor_lifetime is None:
            _assert_child_actor_is_dead()
        elif default_actor_lifetime == "detached":
            _assert_child_actor_still_alive()
        else:
            # It's not detached, the child should be dead.
            _assert_child_actor_is_dead()
    ray.shutdown()

if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
