import pytest
import json
from typing import TypeVar

import ray
from ray.dag.dag_node import DAGNode
from ray.dag.input_node import InputNode
from ray import serve
from ray.dag.utils import _DAGNodeNameGenerator
from ray.serve.handle import (
    RayServeSyncHandle,
    _serve_handle_to_json_dict,
    _serve_handle_from_json_dict,
)
from ray.serve._private.json_serde import (
    DAGNodeEncoder,
    dagnode_from_json,
)
from ray.serve.tests.resources.test_modules import (
    Model,
    combine,
    Counter,
    ClassHello,
    fn_hello,
    Combine,
    NESTED_HANDLE_KEY,
)
from ray.serve._private.deployment_graph_build import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
    transform_serve_dag_to_serve_executor_dag,
)

RayHandleLike = TypeVar("RayHandleLike")
pytestmark = pytest.mark.asyncio


async def test_non_json_serializable_args():
    """Use non-JSON serializable object in Ray DAG and ensure we throw exception
    with reasonable error messages.
    """

    class MyNonJSONClass:
        def __init__(self, val):
            self.val = val

    ray_dag = combine.bind(MyNonJSONClass(1), MyNonJSONClass(2))
    # General context
    with pytest.raises(
        TypeError,
        match=r"Object of type .* is not JSON serializable",
    ):
        _ = json.dumps(ray_dag, cls=DAGNodeEncoder)


async def test_simple_function_node_json_serde(serve_instance):
    """
    Test the following behavior
        1) Ray DAG node can go through full JSON serde cycle
        2) Ray DAG node and deserialized DAG node produces same output
        3) Ray DAG node can go through multiple rounds of JSON serde and still
            provides the same value as if it's only JSON serde once
    Against following test cases
        - Simple function with no args
        - Simple function with only args, all primitive types
        - Simple function with args + kwargs, all primitive types
    """
    original_dag_node = combine.bind(1, 2)
    await _test_deployment_json_serde_helper(
        original_dag_node,
        expected_num_deployments=1,
    )

    original_dag_node = combine.bind(1, 2, kwargs_output=3)
    await _test_deployment_json_serde_helper(
        original_dag_node,
        expected_num_deployments=1,
    )

    original_dag_node = fn_hello.bind()
    await _test_deployment_json_serde_helper(
        original_dag_node,
        expected_num_deployments=1,
    )


async def test_simple_class_node_json_serde(serve_instance):
    """
    Test the following behavior
        1) Ray DAG node can go through full JSON serde cycle
        2) Ray DAG node and deserialized DAG node produces same actor instances
            with same method call output
        3) Ray DAG node can go through multiple rounds of JSON serde and still
            provides the same value as if it's only JSON serde once
    Against following test cases
        - Simple class with no args
        - Simple class with only args, all primitive types
        - Simple class with args + kwargs, all primitive types
        - Simple chain of class method calls, all primitive types
    """
    hello_actor = ClassHello.bind()
    original_dag_node = hello_actor.hello.bind()
    await _test_deployment_json_serde_helper(
        original_dag_node,
        expected_num_deployments=1,
    )

    model_actor = Model.bind(1)
    original_dag_node = model_actor.forward.bind(1)
    await _test_deployment_json_serde_helper(
        original_dag_node,
        expected_num_deployments=1,
    )

    model_actor = Model.bind(1, ratio=0.5)
    original_dag_node = model_actor.forward.bind(1)
    await _test_deployment_json_serde_helper(
        original_dag_node,
        expected_num_deployments=1,
    )


async def _test_deployment_json_serde_helper(
    ray_dag: DAGNode, input=None, expected_num_deployments=None
):
    """Helper function for DeploymentNode and DeploymentMethodNode calls, checks
    the following:
        1) Transform ray dag to serve dag, and ensure serve dag is JSON
            serializable.
        2) Serve dag JSON and be deserialized back to serve dag.
        3) Deserialized serve dag can extract correct number and async definition of
            serve deployments.
    """
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deserialized_deployments = extract_deployments_from_serve_dag(serve_root_dag)
    serve_executor_root_dag = serve_root_dag.apply_recursive(
        transform_serve_dag_to_serve_executor_dag
    )
    json_serialized = json.dumps(serve_executor_root_dag, cls=DAGNodeEncoder)
    deserialized_serve_executor_root_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    assert len(deserialized_deployments) == expected_num_deployments
    # Deploy deserilized version to ensure JSON serde correctness
    for model in deserialized_deployments:
        model.deploy()
    if input is None:
        assert ray.get(ray_dag.execute()) == ray.get(
            await serve_executor_root_dag.execute()
        )
    else:
        assert ray.get(ray_dag.execute(input)) == ray.get(
            await serve_executor_root_dag.execute(input)
        )
    return serve_executor_root_dag, deserialized_serve_executor_root_dag_node


async def test_simple_deployment_method_call_chain(serve_instance):
    """In Ray Core DAG, we maintain a simple linked list to keep track of
    method call lineage on the SAME parent class node with same uuid. However
    JSON serialization is only applicable in serve and we convert all
    ClassMethodNode to DeploymentMethodNode that acts on deployment handle
    that is uniquely identified by its name without dependency of uuid.
    """
    counter = Counter.bind(0)
    counter.inc.bind(1)
    counter.inc.bind(2)
    ray_dag = counter.get.bind()
    assert ray.get(ray_dag.execute()) == 3

    # note(simon): Equivalence is not guaranteed here and
    # nor should it be a supported workflow.

    # (
    #     serve_root_dag,
    #     deserialized_serve_root_dag_node,
    # ) = await _test_deployment_json_serde_helper(ray_dag, expected_num_deployments=1)
    # # Deployment to Deployment, possible DeploymentMethodNode call chain
    # # Both serve dags uses the same underlying deployments, thus the rhs value
    # # went through two execute()
    # assert ray.get(serve_root_dag.execute()) + ray.get(ray_dag.execute()) == ray.get(
    #     deserialized_serve_root_dag_node.execute()
    # )


async def test_multi_instantiation_class_nested_deployment_arg(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        ray_dag = combine.__call__.bind(dag_input)

    (
        serve_root_dag,
        deserialized_serve_root_dag_node,
    ) = await _test_deployment_json_serde_helper(
        ray_dag, input=1, expected_num_deployments=3
    )
    assert ray.get(await serve_root_dag.execute(1)) == ray.get(
        await deserialized_serve_root_dag_node.execute(1)
    )


async def test_nested_deployment_node_json_serde(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)

        m1_output = m1.forward.bind(dag_input)
        m2_output = m2.forward.bind(dag_input)

        ray_dag = combine.bind(m1_output, m2_output)
    (
        serve_root_dag,
        deserialized_serve_root_dag_node,
    ) = await _test_deployment_json_serde_helper(
        ray_dag, input=1, expected_num_deployments=3
    )
    assert ray.get(await serve_root_dag.execute(1)) == ray.get(
        await deserialized_serve_root_dag_node.execute(1)
    )


def get_handle(sync: bool = True):
    @serve.deployment
    def echo(inp: str):
        return inp

    echo.deploy()
    return echo.get_handle(sync=sync)


async def call(handle, inp):
    if isinstance(handle, RayServeSyncHandle):
        ref = handle.remote(inp)
    else:
        ref = await handle.remote(inp)

    return ray.get(ref)


@pytest.mark.asyncio
class TestHandleJSON:
    def test_invalid(self, serve_instance):
        with pytest.raises(ValueError):
            _serve_handle_from_json_dict({"blah": 123})

    @pytest.mark.parametrize("sync", [False, True])
    async def test_basic(self, serve_instance, sync):
        handle = get_handle(sync)
        assert await call(handle, "hi") == "hi"

        serialized = json.dumps(_serve_handle_to_json_dict(handle))
        # Check we can go through multiple rounds of serde.
        serialized = json.dumps(json.loads(serialized))

        # Load the handle back from the dict.
        handle = _serve_handle_from_json_dict(json.loads(serialized))
        assert await call(handle, "hi") == "hi"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
