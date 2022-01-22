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


def test_node_accessors():
    @ray.remote
    def a(*a, **kw):
        pass

    tmp1 = a._bind()
    tmp2 = a._bind()
    tmp3 = a._bind()
    node = a._bind(1, tmp1, x=tmp2, y={"foo": tmp3})

    assert node.get_args() == (1, tmp1)
    assert node.get_kwargs() == {"x": tmp2, "y": {"foo": tmp3}}
    assert node.get_toplevel_child_nodes() == {tmp1, tmp2}
    assert node.get_all_child_nodes() == {tmp1, tmp2, tmp3}

    tmp4 = a._bind()
    tmp5 = a._bind()
    replace = {tmp1: tmp4, tmp2: tmp4, tmp3: tmp5}
    n2 = node.replace_all_child_nodes(lambda x: replace[x])
    assert n2.get_all_child_nodes() == {tmp4, tmp5}


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
