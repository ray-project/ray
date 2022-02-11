import pytest

import ray
from ray.exceptions import RayError
from ray._private.test_utils import wait_for_condition
from ray import serve


@serve.deployment(_health_check_period_s=1, _health_check_timeout_s=1)
class Patient:
    def __init__(self):
        self.healthy = True
        self.should_hang = False

    def check_health(self):
        if self.should_hang:
            import time

            time.sleep(10000)
        elif not self.healthy:
            raise Exception("intended to fail")

    def __call__(self, *args):
        return ray.get_runtime_context().current_actor

    def set_should_fail(self):
        self.healthy = False
        return ray.get_runtime_context().current_actor

    def set_should_hang(self):
        self.should_hang = True
        return ray.get_runtime_context().current_actor


def check_new_actor_started(handle, original_actors):
    if not isinstance(original_actors, set):
        original_actors = {original_actors._actor_id}
    try:
        return ray.get(handle.remote())._actor_id not in original_actors
    except RayError:
        return False


@pytest.mark.parametrize("use_class", [True, False])
def test_no_user_defined_method(serve_instance, use_class):
    """Check the default behavior when an actor crashes."""

    if use_class:

        @serve.deployment
        class A:
            def __call__(self, *args):
                return ray.get_runtime_context().current_actor

    else:

        @serve.deployment
        def A(*args):
            return ray.get_runtime_context().current_actor

    A.deploy()
    h = A.get_handle()
    actor = ray.get(h.remote())
    ray.kill(actor)

    wait_for_condition(check_new_actor_started, handle=h, original_actors=actor)


def test_user_defined_method_fails(serve_instance):
    Patient.deploy()
    h = Patient.get_handle()
    actor = ray.get(h.remote())
    h.set_should_fail.remote()

    wait_for_condition(check_new_actor_started, handle=h, original_actors=actor)
    [ray.get(h.remote()) for _ in range(100)]


def test_user_defined_method_hangs(serve_instance):
    Patient.deploy()
    h = Patient.get_handle()
    actor = ray.get(h.remote())
    h.set_should_hang.remote()

    wait_for_condition(check_new_actor_started, handle=h, original_actors=actor)
    [ray.get(h.remote()) for _ in range(100)]


def test_multiple_replicas(serve_instance):
    Patient.options(num_replicas=2).deploy()
    h = Patient.get_handle()
    actors = {ray.get(h.remote())._actor_id for _ in range(100)}
    assert len(actors) == 2

    h.set_should_fail.remote()

    wait_for_condition(check_new_actor_started, handle=h, original_actors=actors)

    new_actors = {ray.get(h.remote())._actor_id for _ in range(100)}
    assert len(new_actors) == 2
    assert len(new_actors.intersection(actors)) == 1


def test_inherit_healthcheck(serve_instance):
    class Parent:
        def __init__(self):
            self.should_fail = False

        def check_health(self):
            if self.should_fail:
                raise Exception("intended to fail")

        def set_should_fail(self):
            self.should_fail = True

    @serve.deployment
    class Child(Parent):
        def __call__(self, *args):
            return ray.get_runtime_context().current_actor

    Child.deploy()
    h = Child.get_handle()
    actors = {ray.get(h.remote())._actor_id for _ in range(100)}
    assert len(actors) == 1

    h.set_should_fail.remote()
    wait_for_condition(check_new_actor_started, handle=h, original_actors=actors)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
