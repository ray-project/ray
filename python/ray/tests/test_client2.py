import ray
import pytest
from ray.util.client2.session import ClientSession
from ray._private.test_utils import (
    format_web_url,
    get_current_unused_port,
    run_string_as_driver,
)
import subprocess
import os


@ray.remote(num_cpus=0.01)
def fib(j):
    if j < 2:
        return 1
    return sum(ray.get([fib.remote(j - 1), fib.remote(j - 2)]))


@ray.remote
def has_env(k: str, v: str):
    return os.environ[k] == v


@ray.remote
class Counter:
    def __init__(self, initial: int) -> None:
        self.count = initial

    def increment(self, dx):
        self.count += dx
        return self.count

    def total_count(self):
        return self.count


def start_ray_cluster():
    # One need to specify these ports to be able to start more than 1 clusters in a machine.
    port = get_current_unused_port()
    dashboard_port = get_current_unused_port()

    cmd = f"ray start --head --port {port} --dashboard-port {dashboard_port}"
    subprocess.check_output(cmd, shell=True)
    # ray_address = format_web_url(f"http://127.0.0.1:{port}")
    webui = format_web_url(f"http://127.0.0.1:{dashboard_port}")
    # return ray_address, webui
    return webui


@pytest.fixture
def call_ray_start_with_webui_addr():
    # ray_address,
    webui = start_ray_cluster()
    # yield ray_address, webui
    yield webui
    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.check_call(["ray", "stop"])
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


def test_get_put_simple(call_ray_start_with_webui_addr):  # noqa: F811
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_get_put_simple") as client:
        my_data = 42
        ref = client.put(my_data)
        print(f"put {my_data} as ref: {ref}")
        # but within a task/method, it's all in the worker, ofc
        got = client.get(ref)
        print(f"got {got} as ref: {ref}")
        assert type(got) == type(my_data)
        assert got == 42


def test_get_multiple(call_ray_start_with_webui_addr):  # noqa: F811
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_get_put_simple") as client:
        my_data1 = 42
        ref1 = client.put(my_data1)

        my_data2 = "something extra"
        ref2 = client.put(my_data2)

        got1, got2 = client.get([ref1, ref2])
        assert got1 == 42
        assert got2 == "something extra"


class MyDataType:
    def __init__(self, i: int, s: str) -> None:
        self.i = i
        self.s = s

    def pprint(self):
        return f"MyDataType: {self.i}, {self.s}"


def test_get_put_custom_type(call_ray_start_with_webui_addr):  # noqa: F811
    # Get and Put works with custom type.
    # Caveat: the types are within python module "test.py" which does not exist
    # remotely. One have to use the working_dir to let the driver be aware of the module.
    webui = call_ray_start_with_webui_addr
    with ClientSession(
        webui,
        "test_get_put_custom_type",
        runtime_env={"working_dir": os.path.dirname(__file__)},
    ) as client:
        my_data = MyDataType(42, "some serializable python object")
        ref = client.put(my_data)
        print(f"put {my_data.pprint()} as ref: {ref}")
        # but within a task/method, it's all in the worker, ofc
        got = client.get(ref)
        print(f"got {got.pprint()} as ref: {ref}")
        assert type(got) == type(my_data)
        assert got.i == my_data.i
        assert got.s == my_data.s


def test_task_remote(call_ray_start_with_webui_addr):
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_task_remote") as client:
        remote_task_call = fib.remote(5)
        got = client.get(remote_task_call)
        assert got == 8


def test_task_remote_options(call_ray_start_with_webui_addr):
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_task_remote") as client:
        remote_task_call = has_env.options(
            runtime_env={"env_vars": {"test_env_key": "test_env_val"}}
        ).remote("test_env_key", "test_env_val")
        got = client.get(remote_task_call)
        assert got is True


class FibResult:
    def __init__(self, input: int, obj_ref: "ray.ObjectRef[int]") -> None:
        self.obj_ref = obj_ref
        self.input = input


@ray.remote
def fib_with_ref(i):
    obj_ref = fib.remote(i)
    return FibResult(i, obj_ref)


def test_task_remote_custom_type_with_ref(call_ray_start_with_webui_addr):  # noqa: F811
    # Get and Put works with custom type.
    # Caveat: the types are within python module "test.py" which does not exist
    # remotely. One have to use the working_dir to let the driver be aware of the module.
    webui = call_ray_start_with_webui_addr
    with ClientSession(
        webui,
        "test_task_remote_custom_type_with_ref",
        runtime_env={"working_dir": os.path.dirname(__file__)},
    ) as client:
        result_ref = fib_with_ref.remote(5)
        fib_result = client.get(result_ref)
        assert fib_result.input == 5
        assert client.get(fib_result.obj_ref) == 8


@ray.remote(num_cpus=0.01)
def is_prime(n):
    for i in range(2, n // 2 + 1):
        if n % i == 0:
            return False
    return True


@ray.remote(num_cpus=0.01, num_returns="dynamic")
def primes_dynamic(upper_bound):
    for i in range(2, upper_bound):
        if ray.get(is_prime.remote(i)):
            yield i


def test_task_remote_dynamic(call_ray_start_with_webui_addr):
    """
    Although ObjectRefGenerator is implemented in _raylet.pyx, it's a pure python class
    that contains no more than a list of ObjectRefs, and does not require any client2
    code as long as we already handled ObjectRef in custom types correctly. Maybe we can
    move it out of _raylet.pyx.

    On the other side, StreamingObjectRefGenerator has a reference to worker so we need
    to do a client side stub.
    TODO: support StreamingObjectRefGenerator.
    """
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_task_remote") as client:
        remote_task_call = primes_dynamic.remote(100)
        gen = client.get(remote_task_call)
        rets = []
        for ref in gen:
            ret = client.get(ref)
            rets.append(ret)
        assert rets == [
            2,
            3,
            5,
            7,
            11,
            13,
            17,
            19,
            23,
            29,
            31,
            37,
            41,
            43,
            47,
            53,
            59,
            61,
            67,
            71,
            73,
            79,
            83,
            89,
            97,
        ]


def test_actor_remote(call_ray_start_with_webui_addr):
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_actor_remote") as client:
        actor = Counter.remote(5)
        got_ref = actor.increment.remote(3)
        got = client.get(got_ref)
        assert got == 8


def test_reconnection(call_ray_start_with_webui_addr):
    # ClientSession default has detached=True. This means even if the ClientSession is
    # exited, or the whole python interpreter is dead, the Client2DriverActor is still alive
    # and serving. You can reconnect to it and still use the ObjectRef.
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_get_put_cross_reconnection") as client:
        ref = client.put(42)

    with ClientSession(webui, "test_get_put_cross_reconnection") as client:
        got = client.get(ref)
        assert got == 42


def test_reconnection_in_another_process(call_ray_start_with_webui_addr):
    """
    All scripts can connect to a same client2 session by the name. They can visit each
    other's ObjectRef. Main use case: when your script is down and reconnecting.
    """
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "test_reconnection_in_another_process") as client:
        ref = client.put(42)

    driver_script = f"""
import ray
from ray.util.client2.session import ClientSession

webui = "{webui}"
with ClientSession(webui, "test_reconnection_in_another_process") as client:
    ref = ray.ObjectRef({ref.binary()})
    got = client.get(ref)
    print(got)
    """
    assert run_string_as_driver(driver_script).strip() == "42"


def test_no_reconnection_non_detached(call_ray_start_with_webui_addr):
    # If ClientSession has detached=False, client close -> driver killed. Reconnecting
    # only creates another session and all objects/actors are gone.
    webui = call_ray_start_with_webui_addr
    with ClientSession(
        webui, "test_get_put_cross_reconnection", detached=False
    ) as client:
        ref = client.put(42)

    with ClientSession(webui, "test_get_put_cross_reconnection") as client:
        with pytest.raises(ValueError):
            client.get(ref)


def test_move_data_across_clusters(call_ray_start_with_webui_addr):
    """
    With client2 one can do crazy things like moving data from 1 ray cluster to
    another cluster. This is slow though, because there's no 0 copy, and there are serialization, 2
    network transfers, deserialization, serialization, 2 more network transfers, and then
    a deserialization.

    Note: It can move data from 1 Ray cluster to another but in this test we only move within 1 cluster but for 2 client sessions. This is because I did not successfully get the unit test to spin up 2 clusters without interfering each other. It should do fine if we had 2 clusters.
    """
    webui = call_ray_start_with_webui_addr
    with ClientSession(webui, "session1") as client1:
        with ClientSession(webui, "session2") as client2:
            ref1 = client1.put(42)
            obj1 = client1.get(ref1)
            assert obj1 == 42
            ref2 = client2.put(obj1)
            assert client2.get(ref2) == 42
            # task.remote() works in client2
        # task.remote() works in client1


if __name__ == "__main__":
    import sys

    print(ray.__file__)
    sys.exit(pytest.main(["-v", "-s", __file__]))
