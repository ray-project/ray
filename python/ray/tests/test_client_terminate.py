import pytest
import sys
import time

from ray.util.client.ray_client_helpers import ray_start_client_server
from ray.tests.client_test_utils import create_remote_signal_actor
from ray._private.test_utils import wait_for_condition, convert_actor_state
from ray.exceptions import TaskCancelledError
from ray.exceptions import RayTaskError
from ray.exceptions import WorkerCrashedError
from ray.exceptions import ObjectLostError
from ray.exceptions import GetTimeoutError


def valid_exceptions(use_force):
    if use_force:
        return (RayTaskError, TaskCancelledError, WorkerCrashedError, ObjectLostError)
    else:
        return (RayTaskError, TaskCancelledError)


def _all_actors_dead(ray):
    import ray as real_ray
    import ray._private.gcs_utils as gcs_utils

    def _all_actors_dead_internal():
        return all(
            actor["State"] == convert_actor_state(gcs_utils.ActorTableData.DEAD)
            for actor in list(real_ray.state.actors().values())
        )

    return _all_actors_dead_internal


def test_kill_actor_immediately_after_creation(ray_start_regular):
    with ray_start_client_server() as ray:

        @ray.remote
        class A:
            pass

        a = A.remote()
        b = A.remote()

        ray.kill(a)
        ray.kill(b)
        wait_for_condition(_all_actors_dead(ray), timeout=10)


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
@pytest.mark.parametrize("use_force", [True, False])
def test_cancel_chain(ray_start_regular, use_force):
    with ray_start_client_server() as ray:
        SignalActor = create_remote_signal_actor(ray)
        signaler = SignalActor.remote()

        @ray.remote
        def wait_for(t):
            return ray.get(t[0])

        obj1 = wait_for.remote([signaler.wait.remote()])
        obj2 = wait_for.remote([obj1])
        obj3 = wait_for.remote([obj2])
        obj4 = wait_for.remote([obj3])

        assert len(ray.wait([obj1], timeout=0.1)[0]) == 0
        ray.cancel(obj1, force=use_force)
        for ob in [obj1, obj2, obj3, obj4]:
            with pytest.raises(valid_exceptions(use_force)):
                ray.get(ob)

        signaler2 = SignalActor.remote()
        obj1 = wait_for.remote([signaler2.wait.remote()])
        obj2 = wait_for.remote([obj1])
        obj3 = wait_for.remote([obj2])
        obj4 = wait_for.remote([obj3])

        assert len(ray.wait([obj3], timeout=0.1)[0]) == 0
        ray.cancel(obj3, force=use_force)
        for ob in [obj3, obj4]:
            with pytest.raises(valid_exceptions(use_force)):
                ray.get(ob)

        with pytest.raises(GetTimeoutError):
            ray.get(obj1, timeout=0.1)

        with pytest.raises(GetTimeoutError):
            ray.get(obj2, timeout=0.1)

        signaler2.send.remote()
        ray.get(obj1)


def test_kill_cancel_metadata(ray_start_regular):
    """
    Verifies that client worker's terminate_actor and terminate_task methods
    pass worker's metadata attribute server to the grpc stub's Terminate
    method.

    This is done by mocking the grpc stub's Terminate method to raise an
    exception with argument equal to the key of the metadata. We then verify
    that the exception is raised when calling ray.kill and ray.cancel.
    """
    with ray_start_client_server(metadata=[("key", "value")]) as ray:

        @ray.remote
        class A:
            pass

        @ray.remote
        def f():
            time.sleep(1000)

        class MetadataIsCorrectlyPassedException(Exception):
            pass

        def mock_terminate(self, term):
            raise MetadataIsCorrectlyPassedException(self._metadata[1][0])

        # Mock stub's Terminate method to raise an exception.
        stub = ray.get_context().api.worker.data_client
        stub.Terminate = mock_terminate.__get__(stub)

        # Verify the expected exception is raised with ray.kill.
        # Check that argument of the exception matches "key" from the
        # metadata above.
        actor = A.remote()
        with pytest.raises(MetadataIsCorrectlyPassedException, match="key"):
            ray.kill(actor)

        # Verify the expected exception is raised with ray.cancel.
        task_ref = f.remote()
        with pytest.raises(MetadataIsCorrectlyPassedException, match="key"):
            ray.cancel(task_ref)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
