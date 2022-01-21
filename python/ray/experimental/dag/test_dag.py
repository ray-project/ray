import pytest
import pickle

import ray
from ray.experimental.dag.dag_node import DAGNode


@ray.remote
class Counter:
    def __init__(self):
        self.i = 0

    def inc(self):
        self.i += 1

    def get(self):
        return self.i


def test_serialize_warning():
    node = DAGNode([], {})
    with pytest.raises(ValueError):
        pickle.dumps(node)


def test_basic_task_dag():
    ct = Counter.remote()

    @ray.remote
    def a():
        ray.get(ct.inc.remote())
        return 2

    @ray.remote
    def b(x):
        ray.get(ct.inc.remote())
        return x * 2

    @ray.remote
    def c(x):
        ray.get(ct.inc.remote())
        return x + 1

    @ray.remote
    def d(x, y):
        ray.get(ct.inc.remote())
        return x + y

    a_ref = a._bind()
    b_ref = b._bind(a_ref)
    c_ref = c._bind(a_ref)
    dag = d._bind(b_ref, c_ref)
    print(dag.tree_string())

    assert ray.get(dag.execute()) == 7
    assert ray.get(ct.get.remote()) == 4


def test_nested_args():
    ct = Counter.remote()

    @ray.remote
    def a():
        ray.get(ct.inc.remote())
        return 2

    @ray.remote
    def b(**kwargs):
        ray.get(ct.inc.remote())
        return kwargs["x"] * 2

    @ray.remote
    def c(**kwargs):
        ray.get(ct.inc.remote())
        return kwargs["x"] + 1

    @ray.remote
    def d(nested):
        ray.get(ct.inc.remote())
        return ray.get(nested["x"]) + ray.get(nested["y"])

    a_ref = a._bind()
    b_ref = b._bind(x=a_ref)
    c_ref = c._bind(x=a_ref)
    dag = d._bind({"x": b_ref, "y": c_ref})
    print(dag.tree_string())

    assert ray.get(dag.execute()) == 7
    assert ray.get(ct.get.remote()) == 4


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
