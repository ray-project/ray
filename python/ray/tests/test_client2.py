import ray
import pytest
from ray.util.client2.session import start_session
from ray._private.test_utils import (
    format_web_url,
    get_current_unused_port,
)

# Goals:
# - [x] can start actors, run tasks
# - [x] if client dies, the job does not die (can reconnect)
#     - via job submission + detached actor
# - [x] can IO with local desktop files
# - [x] "purely external" to ray core, low maintainence
#       - the whole code should be <1000 LoC.
# - [x] only expose a limited subset of ray
#      - when client is on, ray (as of worker and raylet) is NOT CONNECTED#
#
# Design: use job submission API to spin up a actor that runs http server,
# proxying ray get and put.
#
# Dependant feature: allow to set job metadata from within the job (so that we can tell
# back the http addr) https://github.com/ray-project/ray/issues/36638


######################### usage #####################################################


@ray.remote(num_cpus=0.01)
def fib(j):
    if j < 2:
        return 1
    return sum(ray.get([fib.remote(j - 1), fib.remote(j - 2)]))


class MyDataType:
    def __init__(self, i: int, s: str) -> None:
        self.i = i
        self.s = s

    def pprint(self):
        return f"MyDataType: {self.i}, {self.s}"


@pytest.fixture
def call_ray_start_with_webui_addr():
    import subprocess

    port = get_current_unused_port()
    cmd = "ray start --head " f"--dashboard-port {port}"
    subprocess.check_output(cmd, shell=True)
    webui = format_web_url(f"http://127.0.0.1:{port}")  # TODO: fix this ip
    yield webui
    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.check_call(["ray", "stop"])
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


# TODO: pickle does not work now. It worked in standalone script,
# but now I get:
# ModuleNotFoundError: No module named 'test_client2'
#
@pytest.mark.skip(reason="does not work")
def test_client2_get_put(call_ray_start_with_webui_addr):  # noqa: F811
    webui = call_ray_start_with_webui_addr
    client2_session = start_session(webui, "name")
    with client2_session:
        my_data = MyDataType(42, "some serializable python object")
        ref = ray.put(my_data)
        print(f"put {my_data.pprint()} as ref: {ref}")
        # but within a task/method, it's all in the worker, ofc
        got = ray.get(ref)
        print(f"got {got.pprint()} as ref: {ref}")
        assert type(got) == type(my_data)
        assert got.i == my_data.i
        assert got.s == my_data.s


def test_client2_task_remote(call_ray_start_with_webui_addr):  # noqa: F811
    webui = call_ray_start_with_webui_addr
    client2_session = start_session(webui, "name")
    with client2_session:
        remote_task_call = fib.remote(5)
        print(f"fib.remote(5) = {remote_task_call}")
        got = ray.get(remote_task_call)
        print(f"which evaluates to {got}")
        assert got == 8


if __name__ == "__main__":
    import sys

    print(ray.__file__)
    sys.exit(pytest.main(["-v", "-s", __file__]))
