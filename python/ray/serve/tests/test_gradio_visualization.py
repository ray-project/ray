import pytest

from ray.dag.gradio_visualization import GraphVisualizer

# from ray.serve.drivers import DAGDriver
from ray.dag.utils import _DAGNodeNameGenerator
from ray.dag import (
    # DAGNode,
    InputNode,
    ClassNode,
)
from ray import serve


@pytest.mark.asyncio
async def test_execute_cached_object_ref():
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


def test_get_depth_and_name():
    @serve.deployment
    def f(_, x=0):
        return x

    with InputNode() as user_input:
        input_node = user_input[0]
        f_node = f.bind(input_node)
        dag = f.bind(f_node, input_node)

    visualizer = GraphVisualizer()
    name_generator = _DAGNodeNameGenerator()
    dag.apply_recursive(
        lambda node: visualizer._get_depth_and_name(
            node, name_generator, (InputNode, ClassNode)
        )
    )

    assert (
        visualizer.names[f_node.get_stable_uuid()]
        != visualizer.names[dag.get_stable_uuid()]
    )
    assert (
        visualizer.depths[input_node.get_stable_uuid()] == 0
        and visualizer.depths[f_node.get_stable_uuid()] == 1
        and visualizer.depths[dag.get_stable_uuid()] == 2
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
