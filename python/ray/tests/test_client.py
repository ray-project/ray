import os
import pytest
import time
import sys
import logging
import queue
import threading
import _thread
from unittest.mock import patch
import numpy as np
from ray.util.client.common import OBJECT_TRANSFER_CHUNK_SIZE

import ray.util.client.server.server as ray_client_server
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.tests.client_test_utils import run_wrapped_actor_creation
from ray.util.client.common import ClientObjectRef
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._private.client_mode_hook import client_mode_should_convert
from ray._private.client_mode_hook import disable_client_hook
from ray._private.client_mode_hook import enable_client_mode
from ray._private.test_utils import run_string_as_driver


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_client_context_manager(ray_start_regular_shared, connect_to_client):
    import ray

    with connect_to_client_or_not(connect_to_client):
        if connect_to_client:
            # Client mode is on.
            assert client_mode_should_convert(auto_init=True)
            # We're connected to Ray client.
            assert ray.util.client.ray.is_connected()
        else:
            assert not client_mode_should_convert(auto_init=True)
            assert not ray.util.client.ray.is_connected()


def test_client_thread_safe(call_ray_stop_only):
    import ray

    ray.init(num_cpus=2)

    with ray_start_client_server() as ray:

        @ray.remote
        def block():
            print("blocking run")
            time.sleep(99)

        @ray.remote
        def fast():
            print("fast run")
            return "ok"

        class Blocker(threading.Thread):
            def __init__(self):
                threading.Thread.__init__(self)
                self.daemon = True

            def run(self):
                ray.get(block.remote())

        b = Blocker()
        b.start()
        time.sleep(1)

        # Can concurrently execute the get.
        assert ray.get(fast.remote(), timeout=5) == "ok"


# @pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
# @pytest.mark.skip()
def test_client_mode_hook_thread_safe(ray_start_regular_shared):
    with ray_start_client_server():
        with enable_client_mode():
            assert client_mode_should_convert(auto_init=True)
            lock = threading.Lock()
            lock.acquire()
            q = queue.Queue()

            def disable():
                with disable_client_hook():
                    q.put(client_mode_should_convert(auto_init=True))
                    lock.acquire()
                q.put(client_mode_should_convert(auto_init=True))

            t = threading.Thread(target=disable)
            t.start()
            assert client_mode_should_convert(auto_init=True)
            lock.release()
            t.join()
            assert q.get() is False, "Threaded disable_client_hook failed  to disable"
            assert q.get() is True, "Threaded disable_client_hook failed to re-enable"


def test_interrupt_ray_get(call_ray_stop_only):
    import ray

    ray.init(num_cpus=2)

    with ray_start_client_server() as ray:

        @ray.remote
        def block():
            print("blocking run")
            time.sleep(99)

        @ray.remote
        def fast():
            print("fast run")
            time.sleep(1)
            return "ok"

        class Interrupt(threading.Thread):
            def run(self):
                time.sleep(2)
                _thread.interrupt_main()

        it = Interrupt()
        it.start()
        with pytest.raises(KeyboardInterrupt):
            ray.get(block.remote())

        # Assert we can still get new items after the interrupt.
        assert ray.get(fast.remote()) == "ok"


def test_get_list(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        def f():
            return "OK"

        assert ray.get([]) == []
        assert ray.get([f.remote()]) == ["OK"]

        get_count = 0
        get_stub = ray.worker.server.GetObject

        # ray.get() uses unary-unary RPC. Mock the server handler to count
        # the number of requests received.
        def get(req, metadata=None):
            nonlocal get_count
            get_count += 1
            return get_stub(req, metadata=metadata)

        ray.worker.server.GetObject = get

        refs = [f.remote() for _ in range(100)]
        assert ray.get(refs) == ["OK" for _ in range(100)]

        # Only 1 RPC should be sent.
        assert get_count == 1


def test_real_ray_fallback(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        def get_nodes_real():
            import ray as real_ray

            return real_ray.nodes()

        nodes = ray.get(get_nodes_real.remote())
        assert len(nodes) == 1, nodes

        @ray.remote
        def get_nodes():
            # Can access the full Ray API in remote methods.
            return ray.nodes()

        nodes = ray.get(get_nodes.remote())
        assert len(nodes) == 1, nodes


def test_nested_function(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        def g():
            @ray.remote
            def f():
                return "OK"

            return ray.get(f.remote())

        assert ray.get(g.remote()) == "OK"


def test_put_get(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        objectref = ray.put("hello world")
        print(objectref)

        retval = ray.get(objectref)
        assert retval == "hello world"
        # Make sure ray.put(1) == 1 is False and does not raise an exception.
        objectref = ray.put(1)
        assert not objectref == 1
        # Make sure it returns True when necessary as well.
        assert objectref == ClientObjectRef(objectref.id)
        # Assert output is correct type.
        list_put = ray.put([1, 2, 3])
        assert isinstance(list_put, ClientObjectRef)
        assert ray.get(list_put) == [1, 2, 3]


def test_put_failure_get(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        class DeSerializationFailure:
            def __getstate__(self):
                return ""

            def __setstate__(self, i):
                raise ZeroDivisionError

        dsf = DeSerializationFailure()
        with pytest.raises(ZeroDivisionError):
            ray.put(dsf)

        # Ensure Ray Client is still connected
        assert ray.get(ray.put(100)) == 100


def test_wait(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        objectref = ray.put("hello world")
        ready, remaining = ray.wait([objectref])
        assert remaining == []
        retval = ray.get(ready[0])
        assert retval == "hello world"

        objectref2 = ray.put(5)
        ready, remaining = ray.wait([objectref, objectref2])
        assert (ready, remaining) == ([objectref], [objectref2]) or (
            ready,
            remaining,
        ) == ([objectref2], [objectref])
        ready_retval = ray.get(ready[0])
        remaining_retval = ray.get(remaining[0])
        assert (ready_retval, remaining_retval) == ("hello world", 5) or (
            ready_retval,
            remaining_retval,
        ) == (5, "hello world")

        with pytest.raises(Exception):
            # Reference not in the object store.
            ray.wait([ClientObjectRef(b"blabla")])
        with pytest.raises(TypeError):
            ray.wait("blabla")
        with pytest.raises(TypeError):
            ray.wait(ClientObjectRef("blabla"))
        with pytest.raises(TypeError):
            ray.wait(["blabla"])


def test_remote_functions(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        SignalActor = create_remote_signal_actor(ray)
        signaler = SignalActor.remote()

        @ray.remote
        def plus2(x):
            return x + 2

        @ray.remote
        def fact(x):
            print(x, type(fact))
            if x <= 0:
                return 1
            # This hits the "nested tasks" issue
            # https://github.com/ray-project/ray/issues/3644
            # So we're on the right track!
            return ray.get(fact.remote(x - 1)) * x

        ref2 = plus2.remote(234)
        # `236`
        assert ray.get(ref2) == 236

        ref3 = fact.remote(20)
        # `2432902008176640000`
        assert ray.get(ref3) == 2_432_902_008_176_640_000

        # Reuse the cached ClientRemoteFunc object
        ref4 = fact.remote(5)
        assert ray.get(ref4) == 120

        # Test ray.wait()
        ref5 = fact.remote(10)
        # should return ref2, ref3, ref4
        res = ray.wait([ref5, ref2, ref3, ref4], num_returns=3)
        assert [ref2, ref3, ref4] == res[0]
        assert [ref5] == res[1]
        assert ray.get(res[0]) == [236, 2_432_902_008_176_640_000, 120]
        # should return ref2, ref3, ref4, ref5
        res = ray.wait([ref2, ref3, ref4, ref5], num_returns=4)
        assert [ref2, ref3, ref4, ref5] == res[0]
        assert [] == res[1]
        all_vals = ray.get(res[0])
        assert all_vals == [236, 2_432_902_008_176_640_000, 120, 3628800]

        # Timeout 0 on ray.wait leads to immediate return
        # (not indefinite wait for first return as with timeout None):
        unready_ref = signaler.wait.remote()
        res = ray.wait([unready_ref], timeout=0)
        # Not ready.
        assert res[0] == [] and len(res[1]) == 1
        ray.get(signaler.send.remote())
        ready_ref = signaler.wait.remote()
        # Ready.
        res = ray.wait([ready_ref], timeout=10)
        assert len(res[0]) == 1 and res[1] == []


def test_function_calling_function(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        def g():
            return "OK"

        @ray.remote
        def f():
            print(f, g)
            return ray.get(g.remote())

        print(f, type(f))
        assert ray.get(f.remote()) == "OK"


def test_basic_actor(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        class HelloActor:
            def __init__(self):
                self.count = 0

            def say_hello(self, whom):
                self.count += 1
                return "Hello " + whom, self.count

            @ray.method(num_returns=2)
            def say_hi(self, whom):
                self.count += 1
                return "Hi " + whom, self.count

        actor = HelloActor.remote()
        s, count = ray.get(actor.say_hello.remote("you"))
        assert s == "Hello you"
        assert count == 1

        ref = actor.say_hello.remote("world")
        s, count = ray.get(ref)
        assert s == "Hello world"
        assert count == 2

        r1, r2 = actor.say_hi.remote("ray")
        assert ray.get(r1) == "Hi ray"
        assert ray.get(r2) == 3


def test_pass_handles(ray_start_regular_shared):
    """Test that passing client handles to actors and functions to remote actors
    in functions (on the server or raylet side) works transparently to the
    caller.
    """
    with ray_start_client_server() as ray:

        @ray.remote
        class ExecActor:
            def exec(self, f, x):
                return ray.get(f.remote(x))

            def exec_exec(self, actor, f, x):
                return ray.get(actor.exec.remote(f, x))

        @ray.remote
        def fact(x):
            out = 1
            while x > 0:
                out = out * x
                x -= 1
            return out

        @ray.remote
        def func_exec(f, x):
            return ray.get(f.remote(x))

        @ray.remote
        def func_actor_exec(actor, f, x):
            return ray.get(actor.exec.remote(f, x))

        @ray.remote
        def sneaky_func_exec(obj, x):
            return ray.get(obj["f"].remote(x))

        @ray.remote
        def sneaky_actor_exec(obj, x):
            return ray.get(obj["actor"].exec.remote(obj["f"], x))

        def local_fact(x):
            if x <= 0:
                return 1
            return x * local_fact(x - 1)

        assert ray.get(fact.remote(7)) == local_fact(7)
        assert ray.get(func_exec.remote(fact, 8)) == local_fact(8)
        test_obj = {}
        test_obj["f"] = fact
        assert ray.get(sneaky_func_exec.remote(test_obj, 5)) == local_fact(5)
        actor_handle = ExecActor.remote()
        assert ray.get(actor_handle.exec.remote(fact, 7)) == local_fact(7)
        assert ray.get(func_actor_exec.remote(actor_handle, fact, 10)) == local_fact(10)
        second_actor = ExecActor.remote()
        assert ray.get(
            actor_handle.exec_exec.remote(second_actor, fact, 9)
        ) == local_fact(9)
        test_actor_obj = {}
        test_actor_obj["actor"] = second_actor
        test_actor_obj["f"] = fact
        assert ray.get(sneaky_actor_exec.remote(test_actor_obj, 4)) == local_fact(4)


def test_basic_log_stream(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        log_msgs = []

        def test_log(level, msg):
            log_msgs.append(msg)

        ray.worker.log_client.log = test_log
        ray.worker.log_client.set_logstream_level(logging.DEBUG)
        # Allow some time to propogate
        time.sleep(1)
        x = ray.put("Foo")
        assert ray.get(x) == "Foo"
        time.sleep(1)
        logs_with_id = [msg for msg in log_msgs if msg.find(x.id.hex()) >= 0]
        assert len(logs_with_id) >= 2, logs_with_id
        assert any((msg.find("get") >= 0 for msg in logs_with_id)), logs_with_id
        assert any((msg.find("put") >= 0 for msg in logs_with_id)), logs_with_id


def test_stdout_log_stream(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        log_msgs = []

        def test_log(level, msg):
            log_msgs.append(msg)

        ray.worker.log_client.stdstream = test_log

        @ray.remote
        def print_on_stderr_and_stdout(s):
            print(s)
            print(s, file=sys.stderr)

        time.sleep(1)
        print_on_stderr_and_stdout.remote("Hello world")
        time.sleep(1)
        num_hello = 0
        for msg in log_msgs:
            if "Hello world" in msg:
                num_hello += 1
        assert num_hello == 2, f"Invalid logs: {log_msgs}"


def test_serializing_exceptions(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        with pytest.raises(ValueError, match="Failed to look up actor with name 'abc'"):
            ray.get_actor("abc")


def test_invalid_task(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        with pytest.raises(TypeError):

            @ray.remote(runtime_env="invalid value")
            def f():
                return 1


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


def test_basic_named_actor(ray_start_regular_shared):
    """Test that ray.get_actor() can create and return a detached actor."""
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

        actor = Accumulator.options(name="test_acc2", lifetime="detached").remote()
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
        assert ray._internal_kv_get("apple") is None


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
    thread = threading.Thread(target=stop_server, args=(server,))
    thread.start()
    x = f.remote(2)
    with pytest.raises(ConnectionError):
        _ = ray_client.get(x)
    thread.join()
    ray_client.disconnect()
    ray_client._inside_client_test = False
    # Wait for f(x) to finish before ray.shutdown() in the fixture
    time.sleep(3)


@patch.dict(os.environ, {"RAY_ENABLE_AUTO_CONNECT": "0"})
def test_client_gpu_ids(call_ray_stop_only):
    import ray

    ray.init(num_cpus=2)

    with enable_client_mode():
        # No client connection.
        with pytest.raises(Exception) as e:
            ray.get_gpu_ids()
        assert (
            str(e.value) == "Ray Client is not connected."
            " Please connect by calling `ray.init`."
        )

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


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25552 --port 0"],
    indirect=True,
)
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
    indirect=True,
)
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


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25553 --num-cpus 1"],
    indirect=True,
)
def test_object_ref_release(call_ray_start):
    import ray

    ray.init("ray://localhost:25553")

    a = ray.put("Hello")

    ray.shutdown()
    ray.init("ray://localhost:25553")

    del a

    with disable_client_hook():
        ref_cnt = ray.util.client.ray.get_context().client_worker.reference_count
        assert all(v > 0 for v in ref_cnt.values())


def test_empty_objects(ray_start_regular_shared):
    """
    Tests that client works with "empty" objects. Sanity check, since put requests
    will fail if the serialized version of an object consists of zero bytes.
    """
    objects = [0, b"", "", [], np.array(()), {}, set(), None]
    with ray_start_client_server() as ray:
        for obj in objects:
            ref = ray.put(obj)
            if isinstance(obj, np.ndarray):
                assert np.array_equal(ray.get(ref), obj)
            else:
                assert ray.get(ref) == obj


def test_large_remote_call(ray_start_regular_shared):
    """
    Test remote calls with large (multiple chunk) arguments
    """
    with ray_start_client_server() as ray:

        @ray.remote
        def f(large_obj):
            return large_obj.shape

        @ray.remote
        def f2(*args):
            assert args[0] == 123
            return args[1].shape

        @ray.remote
        def f3(*args, **kwargs):
            assert args[0] == "a"
            assert args[1] == "b"
            return kwargs["large_obj"].shape

        # 1024x1024x16 f64's =~ 128 MiB. Chunking size is 64 MiB, so guarantees
        # that transferring argument requires multiple chunks.
        assert OBJECT_TRANSFER_CHUNK_SIZE < 2 ** 20 * 128
        large_obj = np.random.random((1024, 1024, 16))
        assert ray.get(f.remote(large_obj)) == (1024, 1024, 16)
        assert ray.get(f2.remote(123, large_obj)) == (1024, 1024, 16)
        assert ray.get(f3.remote("a", "b", large_obj=large_obj)) == (1024, 1024, 16)

        @ray.remote
        class SomeActor:
            def __init__(self, large_obj):
                self.inner = large_obj

            def some_method(self, large_obj):
                return large_obj.shape == self.inner.shape

        a = SomeActor.remote(large_obj)
        assert ray.get(a.some_method.remote(large_obj))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
