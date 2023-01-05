import pytest

import ray
from ray.exceptions import RayError
from ray._private.test_utils import wait_for_condition
from ray import serve
from ray.serve._private.common import DeploymentStatus
from ray.serve._private.constants import REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD


class Counter:
    def __init__(self):
        self._count = 0

    def get(self):
        return self._count

    def inc(self):
        self._count += 1
        return self._count

    def reset(self):
        self._count = 0


@serve.deployment(health_check_period_s=1, health_check_timeout_s=1)
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

    h = serve.run(A.bind())
    actor = ray.get(h.remote())
    ray.kill(actor)

    # This would time out if we wait for multiple health check failures.
    wait_for_condition(check_new_actor_started, handle=h, original_actors=actor)


def test_user_defined_method_fails(serve_instance):
    h = serve.run(Patient.bind())
    actor = ray.get(h.remote())
    ray.get(h.set_should_fail.remote())

    wait_for_condition(check_new_actor_started, handle=h, original_actors=actor)
    ray.get([h.remote() for _ in range(100)])


def test_user_defined_method_hangs(serve_instance):
    h = serve.run(Patient.bind())
    actor = ray.get(h.remote())
    ray.get(h.set_should_hang.remote())

    wait_for_condition(check_new_actor_started, handle=h, original_actors=actor)
    ray.get([h.remote() for _ in range(100)])


def test_multiple_replicas(serve_instance):
    h = serve.run(Patient.options(num_replicas=2).bind())
    actors = {a._actor_id for a in ray.get([h.remote() for _ in range(100)])}
    assert len(actors) == 2

    ray.get(h.set_should_fail.remote())

    wait_for_condition(check_new_actor_started, handle=h, original_actors=actors)

    new_actors = {a._actor_id for a in ray.get([h.remote() for _ in range(100)])}
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

    @serve.deployment(health_check_period_s=1)
    class Child(Parent):
        def __call__(self, *args):
            return ray.get_runtime_context().current_actor

    h = serve.run(Child.bind())
    actors = {ray.get(h.remote())._actor_id for _ in range(100)}
    assert len(actors) == 1

    ray.get(h.set_should_fail.remote())
    wait_for_condition(check_new_actor_started, handle=h, original_actors=actors)


def test_nonconsecutive_failures(serve_instance):
    counter = ray.remote(Counter).remote()

    # Test that a health check failing every other call isn't marked unhealthy.
    @serve.deployment(health_check_period_s=0.1)
    class FlakyHealthCheck:
        def check_health(self):
            curr_count = ray.get(counter.inc.remote())
            if curr_count % 2 == 0:
                raise Exception("Ah! I had evens!")

        def __call__(self, *args):
            return ray.get_runtime_context().current_actor

    h = serve.run(FlakyHealthCheck.bind())
    a1 = ray.get(h.remote())

    # Wait for 10 health check periods, should never get marked unhealthy.
    wait_for_condition(lambda: ray.get(counter.get.remote()) > 10)
    assert ray.get(h.remote())._actor_id == a1._actor_id


def test_consecutive_failures(serve_instance):
    # Test that the health check must fail N times before being restarted.

    counter = ray.remote(Counter).remote()

    @serve.deployment(health_check_period_s=1)
    class ChronicallyUnhealthy:
        def __init__(self):
            self._actor_id = ray.get_runtime_context().current_actor._actor_id
            self._should_fail = False

        def check_health(self):
            if self._should_fail:
                ray.get(counter.inc.remote())
                raise Exception("intended to fail")

        def set_should_fail(self):
            self._should_fail = True
            return self._actor_id

        def __call__(self, *args):
            return self._actor_id

    h = serve.run(ChronicallyUnhealthy.bind())

    def check_fails_3_times():
        original_actor_id = ray.get(h.set_should_fail.remote())

        # Wait until a new actor is started.
        wait_for_condition(lambda: ray.get(h.remote()) != original_actor_id)

        # Check that the health check failed N times before replica was killed.
        assert ray.get(counter.get.remote()) == REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD

    # Run the check twice to see that the counter gets reset after a
    # replica is killed.
    check_fails_3_times()
    ray.get(counter.reset.remote())
    check_fails_3_times()


def test_health_check_failure_makes_deployment_unhealthy(serve_instance):
    """If a deployment always fails health check, the deployment should be unhealthy."""

    @serve.deployment
    class AlwaysUnhealthy:
        def check_health(self):
            raise Exception("intended to fail")

        def __call__(self, *args):
            return ray.get_runtime_context().current_actor

    with pytest.raises(RuntimeError):
        serve.run(AlwaysUnhealthy.bind())

    app_status = serve_instance.get_serve_status()
    assert (
        app_status.deployment_statuses[0].name == "AlwaysUnhealthy"
        and app_status.deployment_statuses[0].status == DeploymentStatus.UNHEALTHY
    )


def test_health_check_failure_makes_deployment_unhealthy_transition(serve_instance):
    """
    If a deployment transitions to unhealthy, then continues to fail health check after
    being restarted, the deployment should be unhealthy.
    """

    class Toggle:
        def __init__(self):
            self._should_fail = False

        def set_should_fail(self):
            self._should_fail = True

        def should_fail(self):
            return self._should_fail

    @serve.deployment(health_check_period_s=1, health_check_timeout_s=1)
    class WillBeUnhealthy:
        def __init__(self, toggle):
            self._toggle = toggle

        def check_health(self):
            if ray.get(self._toggle.should_fail.remote()):
                raise Exception("intended to fail")

        def __call__(self, *args):
            return ray.get_runtime_context().current_actor

    def check_status(expected_status: DeploymentStatus):
        app_status = serve_instance.get_serve_status()
        return (
            app_status.deployment_statuses[0].name == "WillBeUnhealthy"
            and app_status.deployment_statuses[0].status == expected_status
        )

    toggle = ray.remote(Toggle).remote()
    serve.run(WillBeUnhealthy.bind(toggle))

    # Check that deployment is healthy initially
    assert check_status(DeploymentStatus.HEALTHY)

    ray.get(toggle.set_should_fail.remote())

    # Check that deployment is now unhealthy
    wait_for_condition(check_status, expected_status=DeploymentStatus.UNHEALTHY)

    # Check that deployment stays unhealthy
    for _ in range(5):
        assert check_status(DeploymentStatus.UNHEALTHY)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
