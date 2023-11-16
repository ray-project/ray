import pytest

import ray
from ray.dag.input_node import InputNode
from ray.dag.output_node import OutputNode
from ray.dag import (
    PARENT_CLASS_NODE_KEY,
    PREV_CLASS_METHOD_CALL_KEY,
)
from ray.dag.vis_utils import plot

def test_output_node(shared_ray_instance):
    @ray.remote
    def f(input):
        return input

    with InputNode() as input_data:
        dag = OutputNode(f.bind(input_data))
    
    assert ray.get(dag.execute(1)) == 1
    assert ray.get(dag.execute(2)) == 2

    with InputNode() as input_data:
        dag = OutputNode([f.bind(input_data["x"]), f.bind(input_data["y"])])
    
    refs = dag.execute({"x": 1, "y": 2})
    assert len(refs) == 2
    assert ray.get(refs) == [1, 2]

    with InputNode() as input_data:
        dag = OutputNode([
            f.bind(input_data["x"]),
            f.bind(input_data["y"]),
            f.bind(input_data["x"])
        ])
    
    refs = dag.execute({"x": 1, "y": 2})
    assert len(refs) == 3
    assert ray.get(refs) == [1, 2, 1]


def test_a(shared_ray_instance):
    @ray.remote
    class Worker:
        def __init__(self):
            pass

        def forward(self, input):
            print("forward")

        def initialize(self, input):
            print("initialize")

    worker = Worker.bind()
    with InputNode() as input_node:
        dag1 = worker.initialize.bind(input_node)
    with InputNode() as input_node:
        dag2 = worker.forward.bind(input_node)

    print(ray.get(dag2.execute(1)))

    # plot(dag1, to_file="a.png")
    # plot(dag2, to_file="b.png")


def test_tensor_parallel_dag(shared_ray_instance):
    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank

        def forward(self, input_data: int):
            print(input_data)
            return self.rank + input_data

        def initialize(self):
            pass

    with InputNode() as input_data:
        workers = [Worker.bind(i) for i in range(4)]
        dag = OutputNode(
            [worker.forward.bind(input_data) for worker in workers])
        init_dag = OutputNode(
            [worker.initialize.bind() for worker in workers])

    # for _ in range(1):
    #     refs = dag.execute(2, _ray_cache_refs=True)
    #     assert len(refs) == 4
    #     all_outputs = ray.get(refs)
    #     assert all_outputs == [2, 3, 4, 5]

    plot(init_dag, to_file="a.png")
    plot(dag, to_file="b.png")
    # ray.get(init_dag.execute(_ray_cache_refs=True))
    import time
    time.sleep(30)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
