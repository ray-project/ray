import pytest
import time
import sys
import logging
import queue
import threading
import _thread

from ray.tests.client_test_utils import create_remote_signal_actor
from ray.util.client.common import ClientObjectRef
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._private.client_mode_hook import client_mode_should_convert
from ray._private.client_mode_hook import disable_client_hook
from ray._private.client_mode_hook import enable_client_mode


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
