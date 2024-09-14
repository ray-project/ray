import random
import ray
import os
import sys
import time
import pytest
from ray.dag import InputNode, MultiOutputNode
import ray.remote_function
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.tests.conftest import *  # noqa
from ray.tests.conftest import wait_for_condition

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


@ray.remote
class Actor:
    def __init__(self, init_value, fail_after=None, sys_exit=False):
        self.i = init_value
        self.fail_after = fail_after
        self.sys_exit = sys_exit

        self.count = 0

    def _fail_if_needed(self):
        if self.fail_after and self.count > self.fail_after:
            # Randomize the failures to better cover multi actor scenarios.
            if random.random() > 0.5:
                if self.sys_exit:
                    os._exit(1)
                else:
                    raise RuntimeError("injected fault")

    def inc(self, x):
        self.i += x
        self.count += 1
        self._fail_if_needed()
        return self.i

    def double_and_inc(self, x):
        self.i *= 2
        self.i += x
        return self.i

    def echo(self, x):
        print("ECHO!")
        self.count += 1
        self._fail_if_needed()
        return x

    def append_to(self, lst):
        lst.append(self.i)
        return lst

    def inc_two(self, x, y):
        self.i += x
        self.i += y
        return self.i

    def sleep(self, x):
        time.sleep(x)
        return x

    @ray.method(num_returns=2)
    def return_two(self, x):
        return x, x + 1


def test_readers_on_different_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    # This node is for the driver (including the CompiledDAG.DAGDriverProxyActor) and
    # one of the readers.
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    # 2 more nodes for other readers.
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    # Wait until nodes actually start, otherwise the code below will fail.
    wait_for_condition(lambda: len(ray.nodes()) == 3)

    a = Actor.options(num_cpus=1).remote(0)
    b = Actor.options(num_cpus=1).remote(0)
    c = Actor.options(num_cpus=1).remote(0)
    actors = [a, b, c]

    def _get_node_id(self) -> "ray.NodeID":
        return ray.get_runtime_context().get_node_id()

    node_ids = ray.get([act.__ray_call__.remote(_get_node_id) for act in actors])
    assert len(set(node_ids)) == 3

    with InputNode() as inp:
        x = a.inc.bind(inp)
        y = b.inc.bind(inp)
        z = c.inc.bind(inp)
        dag = MultiOutputNode([x, y, z])

    adag = dag.experimental_compile()

    for i in range(1, 10):
        assert ray.get(adag.execute(1)) == [i, i, i]

    adag.teardown()


def test_bunch_readers_on_different_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    ACTORS_PER_NODE = 2
    NUM_REMOTE_NODES = 2
    # driver node
    cluster.add_node(num_cpus=ACTORS_PER_NODE)
    ray.init(address=cluster.address)
    # additional nodes for multi readers in multi nodes
    for _ in range(NUM_REMOTE_NODES):
        cluster.add_node(num_cpus=ACTORS_PER_NODE)
    cluster.wait_for_nodes()

    wait_for_condition(lambda: len(ray.nodes()) == NUM_REMOTE_NODES + 1)

    actors = [
        Actor.options(num_cpus=1).remote(0)
        for _ in range(ACTORS_PER_NODE * (NUM_REMOTE_NODES + 1))
    ]

    def _get_node_id(self) -> "ray.NodeID":
        return ray.get_runtime_context().get_node_id()

    node_ids = ray.get([act.__ray_call__.remote(_get_node_id) for act in actors])
    assert len(set(node_ids)) == NUM_REMOTE_NODES + 1

    with InputNode() as inp:
        outputs = []
        for actor in actors:
            outputs.append(actor.inc.bind(inp))
        dag = MultiOutputNode(outputs)

    adag = dag.experimental_compile()

    for i in range(1, 10):
        assert ray.get(adag.execute(1)) == [
            i for _ in range(ACTORS_PER_NODE * (NUM_REMOTE_NODES + 1))
        ]

    adag.teardown()


@pytest.mark.parametrize("single_fetch", [True, False])
def test_pp(ray_start_cluster, single_fetch):
    cluster = ray_start_cluster
    # This node is for the driver.
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    TP = 2
    # This node is for the PP stage 1.
    cluster.add_node(resources={"pp1": TP})
    # This node is for the PP stage 2.
    cluster.add_node(resources={"pp2": TP})

    @ray.remote
    class Worker:
        def __init__(self):
            pass

        def execute_model(self, val):
            return val

    pp1_workers = [
        Worker.options(num_cpus=0, resources={"pp1": 1}).remote() for _ in range(TP)
    ]
    pp2_workers = [
        Worker.options(num_cpus=0, resources={"pp2": 1}).remote() for _ in range(TP)
    ]

    with InputNode() as inp:
        outputs = [inp for _ in range(TP)]
        outputs = [pp1_workers[i].execute_model.bind(outputs[i]) for i in range(TP)]
        outputs = [pp2_workers[i].execute_model.bind(outputs[i]) for i in range(TP)]
        dag = MultiOutputNode(outputs)

    compiled_dag = dag.experimental_compile()
    refs = compiled_dag.execute(1)
    if single_fetch:
        for i in range(TP):
            assert ray.get(refs[i]) == 1
    else:
        assert ray.get(refs) == [1] * TP

    # So that raylets' error messages are printed to the driver
    time.sleep(2)

    compiled_dag.teardown()


def test_payload_large(ray_start_cluster, monkeypatch):
    GRPC_MAX_SIZE = 1024 * 1024 * 5
    monkeypatch.setenv("RAY_max_grpc_message_size", str(GRPC_MAX_SIZE))
    cluster = ray_start_cluster
    # This node is for the driver (including the CompiledDAG.DAGDriverProxyActor).
    first_node_handle = cluster.add_node(num_cpus=1)
    # This node is for the reader.
    second_node_handle = cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    nodes = [first_node_handle.node_id, second_node_handle.node_id]
    # We want to check that there are two nodes. Thus, we convert `nodes` to a set and
    # then back to a list to remove duplicates. Then we check that the length of `nodes`
    # is 2.
    nodes = list(set(nodes))
    assert len(nodes) == 2

    def create_actor(node):
        return Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False)
        ).remote(0)

    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    driver_node = get_node_id(None)
    nodes.remove(driver_node)

    a = create_actor(nodes[0])
    a_node = ray.get(a.__ray_call__.remote(get_node_id))
    assert a_node == nodes[0]
    # Check that the driver and actor are on different nodes.
    assert driver_node != a_node

    with InputNode() as i:
        dag = a.echo.bind(i)

    compiled_dag = dag.experimental_compile()

    size = GRPC_MAX_SIZE + (1024 * 1024 * 2)
    val = b"x" * size

    for i in range(3):
        ref = compiled_dag.execute(val)
        result = ray.get(ref)
        assert result == val

    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


@pytest.mark.parametrize("num_actors", [1, 4])
@pytest.mark.parametrize("num_nodes", [1, 4])
def test_multi_node_multi_reader_large_payload(
    ray_start_cluster, num_actors, num_nodes, monkeypatch
):
    # Set max grpc size to 5mb.
    GRPC_MAX_SIZE = 1024 * 1024 * 5
    monkeypatch.setenv("RAY_max_grpc_message_size", str(GRPC_MAX_SIZE))
    cluster = ray_start_cluster
    ACTORS_PER_NODE = num_actors
    NUM_REMOTE_NODES = num_nodes
    cluster.add_node(num_cpus=ACTORS_PER_NODE)
    ray.init(address=cluster.address)
    # This node is for the other two readers.
    for _ in range(NUM_REMOTE_NODES):
        cluster.add_node(num_cpus=ACTORS_PER_NODE)
    cluster.wait_for_nodes()

    wait_for_condition(lambda: len(ray.nodes()) == NUM_REMOTE_NODES + 1)

    actors = [
        Actor.options(num_cpus=1).remote(0)
        for _ in range(ACTORS_PER_NODE * (NUM_REMOTE_NODES + 1))
    ]

    def _get_node_id(self) -> "ray.NodeID":
        return ray.get_runtime_context().get_node_id()

    node_ids = ray.get([act.__ray_call__.remote(_get_node_id) for act in actors])
    assert len(set(node_ids)) == NUM_REMOTE_NODES + 1

    with InputNode() as inp:
        outputs = []
        for actor in actors:
            outputs.append(actor.echo.bind(inp))
        dag = MultiOutputNode(outputs)

    compiled_dag = dag.experimental_compile()

    # Set the object size a little bigger than the gRPC size so that
    # it triggers chunking and resizing.
    size = GRPC_MAX_SIZE + (1024 * 1024 * 2)
    val = b"x" * size

    for _ in range(3):
        ref = compiled_dag.execute(val)
        result = ray.get(ref)
        assert result == [val for _ in range(ACTORS_PER_NODE * (NUM_REMOTE_NODES + 1))]

    compiled_dag.teardown()


def test_multi_node_dag_from_actor(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init()
    cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=0)
    class SameNodeActor:
        def predict(self, x: str):
            return x

    @ray.remote(num_cpus=1)
    class RemoteNodeActor:
        def predict(self, x: str, y: str):
            return y

    @ray.remote(num_cpus=1)
    class DriverActor:
        def __init__(self):
            self._base_actor = SameNodeActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.get_runtime_context().get_node_id(), soft=False
                )
            ).remote()
            self._refiner_actor = RemoteNodeActor.remote()

            with InputNode() as inp:
                x = self._base_actor.predict.bind(inp)
                dag = self._refiner_actor.predict.bind(
                    inp,
                    x,
                )

            self._adag = dag.experimental_compile(
                _execution_timeout=120,
            )

        def call(self, prompt: str) -> bytes:
            return ray.get(self._adag.execute(prompt))

    parallel = DriverActor.remote()
    assert ray.get(parallel.call.remote("abc")) == "abc"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
