from ray.tests.conftest import *  # noqa

import pytest

import ray
from ray import workflow
from ray.experimental.dag import InputNode


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
    def end(l, r, b):
        return f"{l},{r};{b}"

    with InputNode() as dag_input:
        f = begin._bind(2, dag_input[1], a=dag_input.a)
        l = left._bind(f, "hello", dag_input.a)
        r = right._bind(f, b=dag_input.b, pos=dag_input[0])
        b = end._bind(l, r, b=dag_input.b)

    wf = workflow.create(b, 2, 3.14, a=10, b="ok")
    assert wf.run() == "left(23.14, hello, 10),right(23.14, ok, 2);ok"


def test_dag_to_workflow_options(workflow_start_regular_shared):
    """This test if the workflow inherits DAG options."""

    @ray.remote
    def no_resource():
        pass

    # TODO(suquark): The current Ray DAG is buggy, it failed to return the
    # "original" options, we need to override "num_returns" to pass workflow check.
    dag = no_resource.options(num_gpus=100, num_returns=1)._bind()

    wf = workflow.create(dag)
    assert wf.data.step_options.ray_options["num_gpus"] == 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
