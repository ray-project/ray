import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.dag.input_node import InputNode
from ray.dag.output_node import MultiOutputNode
from ray.util.state import list_tasks


def test_output_node(shared_ray_instance):
    @ray.remote
    def f(input):
        return input

    with pytest.raises(ValueError):
        with InputNode() as input_data:
            dag = MultiOutputNode(f.bind(input_data))

    with InputNode() as input_data:
        dag = MultiOutputNode([f.bind(input_data)])

    assert ray.get(dag.execute(1)) == [1]
    assert ray.get(dag.execute(2)) == [2]

    with InputNode() as input_data:
        dag = MultiOutputNode([f.bind(input_data["x"]), f.bind(input_data["y"])])

    refs = dag.execute({"x": 1, "y": 2})
    assert len(refs) == 2
    assert ray.get(refs) == [1, 2]

    with InputNode() as input_data:
        dag = MultiOutputNode(
            [f.bind(input_data["x"]), f.bind(input_data["y"]), f.bind(input_data["x"])]
        )

    refs = dag.execute({"x": 1, "y": 2})
    assert len(refs) == 3
    assert ray.get(refs) == [1, 2, 1]


def test_dag_with_actor_handle(shared_ray_instance):
    """Verify DAG API works with actor created by .remote"""

    @ray.remote
    class Worker:
        def __init__(self):
            self.forward_called = 0
            self.init_called = 0

        def forward(self, input):
            print("forward")
            self.forward_called += 1
            return input

        def initialize(self, input):
            print("initialize")
            self.init_called += 1
            return input

        def get(self):
            return (self.forward_called, self.init_called)

    worker = Worker.remote()
    with InputNode() as input_node:
        init_dag = worker.initialize.bind(input_node)
    with InputNode() as input_node:
        forward_dag = worker.forward.bind(input_node)

    assert ray.get(init_dag.execute(1)) == 1
    assert ray.get(forward_dag.execute(2)) == 2

    # Make sure both forward/initialize called only once
    assert ray.get(worker.get.remote()) == (1, 1)

    # Double check the actor is resued.
    assert ray.get(init_dag.execute(1)) == 1
    assert ray.get(worker.get.remote()) == (1, 2)


def test_dag_with_alive_actors_chained(shared_ray_instance):
    """Verify we can have multiple DAGs to the
    same actor that are chained.
    """

    @ray.remote
    class Actor:
        def __init__(self, init_value):
            self.i = init_value

        def add(self, x):
            return self.i + x

    @ray.remote
    def combine(x, y):
        return x + y

    a1 = Actor.remote(10)
    a1_dag = a1.add.bind(a1.add.bind(2))  # 22
    a1_dag_2 = a1.add.bind(a1.add.bind(6))  # 26
    dag = combine.bind(a1_dag, a1_dag_2)

    assert ray.get(dag.execute()) == 48


def test_tensor_parallel_dag(shared_ray_instance):
    """Simulate the TP DAG with N workers.
    Input -> forward -> MultiOutput
    """

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank
            self.forwarded = 0

        def forward(self, input_data: int):
            print(input_data)
            self.forwarded += 1
            return self.rank + input_data

        def initialize(self):
            pass

        def get_forwarded(self):
            return self.forwarded

    NUM_WORKERS = 4
    workers = [Worker.remote(i) for i in range(NUM_WORKERS)]
    # Init multiple times.
    for _ in range(4):
        ray.get([worker.initialize.remote() for worker in workers])

    with InputNode() as input_data:
        dag = MultiOutputNode([worker.forward.bind(input_data) for worker in workers])

    # Run DAG repetitively.
    ITER = 4
    assert ITER > 1
    for i in range(ITER):
        ref = dag.execute(i)
        all_outputs = ray.get(ref)
        assert len(all_outputs) == NUM_WORKERS
        assert all_outputs == [i + j for j in range(NUM_WORKERS)]

    forwarded = ray.get([worker.get_forwarded.remote() for worker in workers])
    assert forwarded == [ITER for _ in range(NUM_WORKERS)]


def test_shared_output(shared_ray_instance):
    """Verify when an upstream task output is shared by
    multi output, the upstream task runs only once.
    """

    @ray.remote
    def shared_f():
        return 1

    @ray.remote
    def g(input):
        return input + 1

    @ray.remote
    def h(input):
        return input + 2

    x = shared_f.bind()
    dag = MultiOutputNode([g.bind(x), h.bind(x)])

    assert ray.get(dag.execute()) == [2, 3]

    # Verify f ran only once.
    def verify():
        tasks = list_tasks(filters=[("name", "=", "shared_f")])
        return len(tasks) == 1

    wait_for_condition(verify)


def test_bind_survives_handle_deletion(shared_ray_instance):
    """Verify that .bind().execute() still works even if the original handle was dropped."""

    @ray.remote
    class A:
        def f(self):
            return 1

    # Grab the handle and the bound method node
    actor = A.remote()
    method_node = actor.f.bind()

    # Destroy the only Python variable reference and force collection
    del actor

    # Executing should now succeed because the node holds the ref
    result = ray.get(method_node.execute())
    assert result == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
