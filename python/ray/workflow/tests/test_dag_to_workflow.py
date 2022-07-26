from ray.tests.conftest import *  # noqa

import pytest

import ray
from ray import workflow
from ray.dag import InputNode


def test_dag_to_workflow_execution(workflow_start_regular_shared):
    """This test constructs a DAG with complex dependencies
    and turns it into a workflow."""

    @ray.remote
    def begin(x, pos, a):
        return x * a + pos  # 23.14

    @ray.remote
    def left(x, c, a):
        return f"left({x}, {c}, {a})"

    @ray.remote
    def right(x, b, pos):
        return f"right({x}, {b}, {pos})"

    @ray.remote
    def end(lf, rt, b):
        return f"{lf},{rt};{b}"

    with pytest.raises(TypeError):
        workflow.run_async(begin.remote(1, 2, 3))

    with InputNode() as dag_input:
        f = begin.bind(2, dag_input[1], a=dag_input.a)
        lf = left.bind(f, "hello", dag_input.a)
        rt = right.bind(f, b=dag_input.b, pos=dag_input[0])
        b = end.bind(lf, rt, b=dag_input.b)

    assert (
        workflow.run(b, 2, 3.14, a=10, b="ok")
        == "left(23.14, hello, 10),right(23.14, ok, 2);ok"
    )


def test_dedupe_serialization_dag(workflow_start_regular_shared):
    from ray.workflow import serialization
    from ray.workflow.tests.utils import skip_client_mode_test

    # TODO(suquark): Fix workflow with ObjectRefs as inputs under client mode.
    skip_client_mode_test()

    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def gather(*args):
        return args

    def get_num_uploads():
        manager = serialization.get_or_create_manager()
        stats = ray.get(manager.export_stats.remote())
        return stats.get("num_uploads", 0)

    ref = ray.put("hello world 12345")
    list_of_refs = [ref for _ in range(20)]

    assert get_num_uploads() == 0

    single = identity.bind((ref,))
    double = identity.bind(list_of_refs)

    result_ref, result_list = workflow.run(gather.bind(single, double))

    for result in result_list:
        assert ray.get(*result_ref) == ray.get(result)

    # One upload for the initial checkpoint.
    assert get_num_uploads() == 1


def test_same_object_many_dags(workflow_start_regular_shared):
    """Ensure that when we dedupe uploads, we upload the object once per DAG,
    since different DAGs shouldn't look in each others object directories.
    """
    from ray.workflow.tests.utils import skip_client_mode_test

    # TODO(suquark): Fix workflow with ObjectRefs as inputs under client mode.
    skip_client_mode_test()

    @ray.remote
    def f(a):
        return [a[0]]

    x = {0: ray.put(10)}

    result1 = workflow.run(f.bind(x))
    result2 = workflow.run(f.bind(x))
    with InputNode() as dag_input:
        result3 = workflow.run(f.bind(dag_input.x), x=x)

    assert ray.get(*result1) == 10
    assert ray.get(*result2) == 10
    assert ray.get(*result3) == 10


def test_dereference_object_refs(workflow_start_regular_shared):
    """Ensure that object refs are dereferenced like in ray tasks."""
    from ray.workflow.tests.utils import skip_client_mode_test

    # TODO(suquark): Fix workflow with ObjectRefs as inputs under client mode.
    skip_client_mode_test()

    @ray.remote
    def f(obj_list):
        assert isinstance(obj_list[0], ray.ObjectRef)
        assert ray.get(obj_list) == [42]

    @ray.remote
    def g(x, y):
        assert x == 314
        assert isinstance(y[0], ray.ObjectRef)
        assert ray.get(y) == [2022]
        return [ray.put(42)]

    @ray.remote
    def h():
        return ray.put(2022)

    dag = f.bind(g.bind(x=ray.put(314), y=[ray.put(2022)]))

    # Run with workflow and normal Ray engine.
    workflow.run(dag)
    ray.get(dag.execute())


def test_dereference_dags(workflow_start_regular_shared):
    """Ensure that DAGs are dereferenced like ObjectRefs in ray tasks."""

    @ray.remote
    def g(x0, y0, z0, x1, y1, z1):
        assert x0 == 314
        assert isinstance(x1[0], ray.ObjectRef)
        assert ray.get(x1) == [314]

        assert isinstance(y0, ray.ObjectRef)
        assert ray.get(y0) == 271828
        (y10,) = y1
        assert isinstance(y10, ray.ObjectRef)
        assert isinstance(ray.get(y10), ray.ObjectRef)
        assert ray.get(ray.get(y10)) == 271828

        assert z0 == 46692
        assert isinstance(z1[0], ray.ObjectRef)
        assert ray.get(z1) == [46692]

        return "ok"

    @ray.remote
    def h(x):
        return x

    @ray.remote
    def nested(x):
        return h.bind(x).execute()

    @ray.remote
    def nested_continuation(x):
        return workflow.continuation(h.bind(x))

    dag = g.bind(
        x0=h.bind(314),
        y0=nested.bind(271828),
        z0=nested_continuation.bind(46692),
        x1=[h.bind(314)],
        y1=[nested.bind(271828)],
        z1=[nested_continuation.bind(46692)],
    )

    # Run with workflow and normal Ray engine.
    assert workflow.run(dag) == "ok"
    assert ray.get(dag.execute()) == "ok"


def test_workflow_continuation(workflow_start_regular_shared):
    """Test unified behavior of returning continuation inside
    workflow and default Ray execution engine."""

    @ray.remote
    def h(a, b):
        return a + b

    @ray.remote
    def g(x):
        return workflow.continuation(h.bind(42, x))

    @ray.remote
    def f():
        return workflow.continuation(g.bind(1))

    with pytest.raises(TypeError):
        workflow.continuation(f.remote())

    dag = f.bind()
    assert ray.get(dag.execute()) == 43
    assert workflow.run(dag) == 43


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
