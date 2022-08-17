import pytest
from collections import defaultdict

from ray.dag.gradio_visualization import GraphVisualizer

from ray.dag import InputNode
from ray.serve.drivers import DAGDriver
from ray import serve


@pytest.fixture
def graph1():
    @serve.deployment
    def f(x):
        return x

    @serve.deployment
    class model:
        def __init__(self, _):
            pass

        def run(self, x):
            return x

    with InputNode() as user_input:
        f_node = f.bind(user_input[0])
        m = model.bind(f_node)
        dag = m.run.bind(user_input[1])

    yield f_node, m, dag


@pytest.fixture
def graph2():
    @serve.deployment
    def f(_, x=0):
        return x

    with InputNode() as user_input:
        input_node = user_input[0]
        f_node = f.bind(input_node)
        dag = f.bind(f_node, input_node)

    yield input_node, f_node, dag


@pytest.mark.asyncio
async def test_execute_cached_object_ref(graph1):
    """
    Tests DAGNode.get_object_ref_from_last_execute() correctly returns object refs
    to the submitted tasks after DAGNode.execute() is run.
    """

    (f_node, _, dag) = graph1

    dag.execute([1, 2])
    assert (
        await (await dag.get_object_ref_from_last_execute(f_node.get_stable_uuid()))
        == 1
    )
    assert (
        await (await dag.get_object_ref_from_last_execute(dag.get_stable_uuid())) == 2
    )


@pytest.mark.asyncio
async def test_get_result(graph1):
    """
    Tests that after running handle.predict.remote(), _get_result() in GraphVisualizer
    correctly returns object refs to the submitted tasks.
    """
    (_, _, dag) = graph1

    handle = serve.run(DAGDriver.bind(dag))
    visualizer = GraphVisualizer()
    visualizer.visualize_with_gradio(handle, _launch=False)

    handle.predict.remote(1, 2)
    values = [await (visualizer._get_result(uuid)) for uuid in visualizer.node_to_block]
    assert {1, 2} <= set(values)


def test_fetch_depths(graph2):
    """
    Tests that GraphVisualizer._fetch_depths, when passed into
    DAGNode.apply_recursive, correctly retrieves the depths of each node.
    """

    (input_node, f_node, dag) = graph2

    visualizer = GraphVisualizer()
    depths = defaultdict(lambda: 0)
    dag.apply_recursive(lambda node: visualizer._fetch_depths(node, depths))

    assert (
        depths[input_node.get_stable_uuid()] == 1
        and depths[f_node.get_stable_uuid()] == 2
        and depths[dag.get_stable_uuid()] == 3
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
