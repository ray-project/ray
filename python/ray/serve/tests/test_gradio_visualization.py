import pytest
from collections import defaultdict

from ray.dag.gradio_visualization import GraphVisualizer

from ray.dag import InputNode
from ray import serve


@pytest.mark.asyncio
async def test_execute_cached_object_ref():
    """
    Tests DAGNode.get_object_ref_from_last_execute() correclty returns object refs
    to the submitted tasks after DAGNode.execute() is run.
    """

    @serve.deployment
    def f():
        return 1

    @serve.deployment
    class model:
        def __init__(self, _):
            pass

        def run(self):
            return 2

    f_node = f.bind()
    m = model.bind(f_node)
    dag = m.run.bind()

    dag.execute()
    assert (
        await (await dag.get_object_ref_from_last_execute(f_node.get_stable_uuid()))
        == 1
    )
    assert (
        await (await dag.get_object_ref_from_last_execute(dag.get_stable_uuid())) == 2
    )


def test_fetch_depths():
    """
    Tests that GraphVisualizer._fetch_depths, when passed into
    DAGNode.apply_recursive, correctly retrieves the depths of each node.
    """

    @serve.deployment
    def f(_, x=0):
        return x

    with InputNode() as user_input:
        input_node = user_input[0]
        f_node = f.bind(input_node)
        dag = f.bind(f_node, input_node)

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
