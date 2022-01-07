import os
import pytest
import time
import sys
import threading
from unittest.mock import patch

import ray.util.client.server.server as ray_client_server
from ray.tests.client_test_utils import run_wrapped_actor_creation
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._private.client_mode_hook import enable_client_mode
from ray._private.test_utils import run_string_as_driver


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_serializing_exceptions(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        with pytest.raises(
                ValueError, match="Failed to look up actor with name 'abc'"):
            ray.get_actor("abc")


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_invalid_task(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote(runtime_env="invalid value")
        def f():
            return 1

        # No exception on making the remote call.
        ref = f.remote()

        # Exception during scheduling will be raised on ray.get()
        with pytest.raises(Exception):
            ray.get(ref)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_create_remote_before_start(ray_start_regular_shared):
    """Creates remote objects (as though in a library) before
    starting the client.
    """
    from ray.util.client import ray

    @ray.remote
    class Returner:
        def doit(self):
            return "foo"

    @ray.remote
    def f(x):
        return x + 20

    # Prints in verbose tests
    print("Created remote functions")

    with ray_start_client_server() as ray:
        assert ray.get(f.remote(3)) == 23
        a = Returner.remote()
        assert ray.get(a.doit.remote()) == "foo"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_basic_named_actor(ray_start_regular_shared):
    """Test that ray.get_actor() can create and return a detached actor.
    """
    with ray_start_client_server() as ray:

        @ray.remote
        class Accumulator:
            def __init__(self):
                self.x = 0

            def inc(self):
                self.x += 1

            def get(self):
                return self.x

            @ray.method(num_returns=2)
            def half(self):
                return self.x / 2, self.x / 2

        # Create the actor
        actor = Accumulator.options(name="test_acc").remote()

        actor.inc.remote()
        actor.inc.remote()

        # Make sure the get_actor call works
        new_actor = ray.get_actor("test_acc")
        new_actor.inc.remote()
        assert ray.get(new_actor.get.remote()) == 3

        del actor

        actor = Accumulator.options(
            name="test_acc2", lifetime="detached").remote()
        actor.inc.remote()
        del actor

        detatched_actor = ray.get_actor("test_acc2")
        for i in range(5):
            detatched_actor.inc.remote()

        assert ray.get(detatched_actor.get.remote()) == 6

        h1, h2 = ray.get(detatched_actor.half.remote())
        assert h1 == 3
        assert h2 == 3


def test_error_serialization(ray_start_regular_shared):
    """Test that errors will be serialized properly."""
    fake_path = os.path.join(os.path.dirname(__file__), "not_a_real_file")
    with pytest.raises(FileNotFoundError):
        with ray_start_client_server() as ray:

            @ray.remote
            def g():
                with open(fake_path, "r") as f:
                    f.read()

            # Raises a FileNotFoundError
            ray.get(g.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_internal_kv(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        assert ray._internal_kv_initialized()
        assert not ray._internal_kv_put("apple", "b")
        assert ray._internal_kv_put("apple", "asdf")
        assert ray._internal_kv_put("apple", "b")
        assert ray._internal_kv_get("apple") == b"b"
        assert ray._internal_kv_put("apple", "asdf", overwrite=True)
        assert ray._internal_kv_get("apple") == b"asdf"
        assert ray._internal_kv_list("a") == [b"apple"]
        ray._internal_kv_del("apple")
        assert ray._internal_kv_get("apple") == b""


def test_startup_retry(ray_start_regular_shared):
    from ray.util.client import ray as ray_client
    ray_client._inside_client_test = True

    with pytest.raises(ConnectionError):
        ray_client.connect("localhost:50051", connection_retries=1)

    def run_client():
        ray_client.connect("localhost:50051")
        ray_client.disconnect()

    thread = threading.Thread(target=run_client, daemon=True)
    thread.start()
    time.sleep(3)
    server = ray_client_server.serve("localhost:50051")
    thread.join()
    server.stop(0)
    ray_client._inside_client_test = False


def test_dataclient_server_drop(ray_start_regular_shared):
    from ray.util.client import ray as ray_client
    ray_client._inside_client_test = True

    @ray_client.remote
    def f(x):
        time.sleep(4)
        return x

    def stop_server(server):
        time.sleep(2)
        server.stop(0)

    server = ray_client_server.serve("localhost:50051")
    ray_client.connect("localhost:50051")
    thread = threading.Thread(target=stop_server, args=(server, ))
    thread.start()
    x = f.remote(2)
    with pytest.raises(ConnectionError):
        _ = ray_client.get(x)
    thread.join()
    ray_client.disconnect()
    ray_client._inside_client_test = False
    # Wait for f(x) to finish before ray.shutdown() in the fixture
    time.sleep(3)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@patch.dict(os.environ, {"RAY_ENABLE_AUTO_CONNECT": "0"})
def test_client_gpu_ids(call_ray_stop_only):
    import ray
    ray.init(num_cpus=2)

    with enable_client_mode():
        # No client connection.
        with pytest.raises(Exception) as e:
            ray.get_gpu_ids()
        assert str(e.value) == "Ray Client is not connected."\
            " Please connect by calling `ray.init`."

        with ray_start_client_server():
            # Now have a client connection.
            assert ray.get_gpu_ids() == []


def test_client_serialize_addon(call_ray_stop_only):
    import ray
    import pydantic

    ray.init(num_cpus=0)

    class User(pydantic.BaseModel):
        name: str

    with ray_start_client_server() as ray:
        assert ray.get(ray.put(User(name="ray"))).name == "ray"


object_ref_cleanup_script = """
import ray

ray.init("ray://localhost:50051")

@ray.remote
def f():
    return 42

@ray.remote
class SomeClass:
    pass


obj_ref = f.remote()
actor_ref = SomeClass.remote()
"""


def test_object_ref_cleanup():
    # Checks no error output when running the script in
    # object_ref_cleanup_script
    # See https://github.com/ray-project/ray/issues/17968 for details
    with ray_start_client_server():
        result = run_string_as_driver(object_ref_cleanup_script)
        assert "Error in sys.excepthook:" not in result
        assert "AttributeError: 'NoneType' object has no " not in result
        assert "Exception ignored in" not in result


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25552 --port 0"],
    indirect=True)
def test_wrapped_actor_creation(call_ray_start):
    """
    When the client schedules an actor, the server will load a separate
    copy of the actor class if it's defined in a separate file. This
    means that modifications to the client's copy of the actor class
    aren't propagated to the server. Currently, tracing logic modifies
    the signatures of actor methods to pass around metadata when ray.remote
    is applied to an actor class. However, if a user does something like:

    class SomeActor:
        def __init__(self):
            pass

    def decorate_actor():
        RemoteActor = ray.remote(SomeActor)
        ...

    Then the SomeActor class will have its signatures modified on the client
    side, but not on the server side, since ray.remote was applied inside of
    the function instead of directly on the actor. Note if it were directly
    applied to the actor then the signature would be modified when the server
    imports the class.
    """
    import ray
    ray.init("ray://localhost:25552")
    run_wrapped_actor_creation()


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25553 --num-cpus 0"],
    indirect=True)
@pytest.mark.parametrize("use_client", [True, False])
def test_init_requires_no_resources(call_ray_start, use_client):
    import ray
    if use_client:
        address = call_ray_start
        ray.init(address)
    else:
        ray.init("ray://localhost:25553")

    @ray.remote(num_cpus=0)
    def f():
        pass

    ray.get(f.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
