import pytest
import json
from typing import TypeVar

import ray
from ray.experimental.dag.dag_node import DAGNode
from ray.experimental.dag.input_node import InputNode
from ray import serve
from ray.experimental.dag.utils import DAGNodeNameGenerator
from ray.serve.handle import (
    RayServeSyncHandle,
    serve_handle_to_json_dict,
    serve_handle_from_json_dict,
)
from ray.serve.pipeline.json_serde import (
    DAGNodeEncoder,
    dagnode_from_json,
    DAGNODE_TYPE_KEY,
)
from ray.serve.pipeline.tests.resources.test_modules import (
    Model,
    combine,
    Counter,
    ClassHello,
    fn,
    fn_hello,
    Combine,
    NESTED_HANDLE_KEY,
)
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
)

RayHandleLike = TypeVar("RayHandleLike")


def _test_execution_class_node_ClassHello(original_dag_node, deserialized_dag_node):
    # Creates actor of ClassHello
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.hello.remote()) == ray.get(
        deserialized_actor.hello.remote()
    )


def _test_execution_class_node_Model(original_dag_node, deserialized_dag_node):
    # Creates actor of Model
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.forward.remote(2)) == ray.get(
        deserialized_actor.forward.remote(2)
    )


def _test_execution_function_node(original_dag_node, deserialized_dag_node):
    assert ray.get(deserialized_dag_node.execute()) == ray.get(
        original_dag_node.execute()
    )


def _test_json_serde_helper(
    original_dag_node, executor_fn=None, expected_json_dict=None
):
    """Helpful function to test full round the the following behavior:
    1) Ray DAG node can go through full JSON serde cycle
    2) Ray DAG node and deserialized DAG node produces same output
    3) Ray DAG node can go through multiple rounds of JSON serde and still
        provides the same value as if it's only JSON serialized once
    """
    json_serialized = json.dumps(original_dag_node, cls=DAGNodeEncoder)
    assert json_serialized == json.dumps(expected_json_dict)
    deserialized_dag_node = json.loads(json_serialized, object_hook=dagnode_from_json)

    executor_fn(original_dag_node, deserialized_dag_node)

    # Ensure same node can produce exactly the same results on FunctionNode
    # from one or multiple rounds of serialization
    multiple_json_serialized = json.dumps(
        json.loads(
            json.dumps(original_dag_node, cls=DAGNodeEncoder),
            object_hook=dagnode_from_json,
        ),
        cls=DAGNodeEncoder,
    )
    assert multiple_json_serialized == json_serialized


def test_non_json_serializable_args():
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


def test_simple_function_node_json_serde(serve_instance):
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
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_function_node,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.resources.test_modules.combine",
            "args": [1, 2],
            "kwargs": {},
            "options": {},
            "other_args_to_resolve": {},
            "uuid": original_dag_node.get_stable_uuid(),
        },
    )

    original_dag_node = combine.bind(1, 2, kwargs_output=3)
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_function_node,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.resources.test_modules.combine",
            "args": [1, 2],
            "kwargs": {"kwargs_output": 3},
            "options": {},
            "other_args_to_resolve": {},
            "uuid": original_dag_node.get_stable_uuid(),
        },
    )

    original_dag_node = fn_hello.bind()
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_function_node,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.resources.test_modules.fn_hello",
            "args": [],
            "kwargs": {},
            "options": {},
            "other_args_to_resolve": {},
            "uuid": original_dag_node.get_stable_uuid(),
        },
    )


def test_simple_class_node_json_serde(serve_instance):
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
    original_dag_node = ClassHello.bind()
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_class_node_ClassHello,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.resources.test_modules.ClassHello",
            "args": [],
            "kwargs": {},
            "options": {},
            "other_args_to_resolve": {},
            "uuid": original_dag_node.get_stable_uuid(),
        },
    )

    original_dag_node = Model.bind(1)
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_class_node_Model,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.resources.test_modules.Model",
            "args": [1],
            "kwargs": {},
            "options": {},
            "other_args_to_resolve": {},
            "uuid": original_dag_node.get_stable_uuid(),
        },
    )

    original_dag_node = Model.bind(1, ratio=0.5)
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_class_node_Model,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.resources.test_modules.Model",
            "args": [1],
            "kwargs": {"ratio": 0.5},
            "options": {},
            "other_args_to_resolve": {},
            "uuid": original_dag_node.get_stable_uuid(),
        },
    )


def _test_deployment_json_serde_helper(
    ray_dag: DAGNode, input=None, expected_num_deployments=None
):
    """Helper function for DeploymentNode and DeploymentMethodNode calls, checks
    the following:
        1) Transform ray dag to serve dag, and ensure serve dag is JSON
            serializable.
        2) Serve dag JSON and be deserialized back to serve dag.
        3) Deserialized serve dag can extract correct number and definition of
            serve deployments.
    """
    with DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    json_serialized = json.dumps(serve_root_dag, cls=DAGNodeEncoder)
    deserialized_serve_root_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    deserialized_deployments = extract_deployments_from_serve_dag(
        deserialized_serve_root_dag_node
    )
    assert len(deserialized_deployments) == expected_num_deployments
    # Deploy deserilized version to ensure JSON serde correctness
    for model in deserialized_deployments:
        model.deploy()
    if input is None:
        assert ray.get(ray_dag.execute()) == ray.get(serve_root_dag.execute())
    else:
        assert ray.get(ray_dag.execute(input)) == ray.get(serve_root_dag.execute(input))
    return serve_root_dag, deserialized_serve_root_dag_node


def test_simple_deployment_method_call_chain(serve_instance):
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
    (
        serve_root_dag,
        deserialized_serve_root_dag_node,
    ) = _test_deployment_json_serde_helper(ray_dag, expected_num_deployments=1)
    # Deployment to Deployment, possible DeploymentMethodNode call chain
    # Both serve dags uses the same underlying deployments, thus the rhs value
    # went through two execute()
    assert ray.get(serve_root_dag.execute()) + ray.get(ray_dag.execute()) == ray.get(
        deserialized_serve_root_dag_node.execute()
    )


def test_multi_instantiation_class_nested_deployment_arg(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        ray_dag = combine.__call__.bind(dag_input)

    (
        serve_root_dag,
        deserialized_serve_root_dag_node,
    ) = _test_deployment_json_serde_helper(ray_dag, input=1, expected_num_deployments=3)
    assert ray.get(serve_root_dag.execute(1)) == ray.get(
        deserialized_serve_root_dag_node.execute(1)
    )


def test_nested_deployment_node_json_serde(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)

        m1_output = m1.forward.bind(dag_input)
        m2_output = m2.forward.bind(dag_input)

        ray_dag = combine.bind(m1_output, m2_output)
    (
        serve_root_dag,
        deserialized_serve_root_dag_node,
    ) = _test_deployment_json_serde_helper(ray_dag, input=1, expected_num_deployments=2)
    assert ray.get(serve_root_dag.execute(1)) == ray.get(
        deserialized_serve_root_dag_node.execute(1)
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
            serve_handle_from_json_dict({"blah": 123})

    @pytest.mark.parametrize("sync", [False, True])
    async def test_basic(self, serve_instance, sync):
        handle = get_handle(sync)
        assert await call(handle, "hi") == "hi"

        serialized = json.dumps(serve_handle_to_json_dict(handle))
        # Check we can go through multiple rounds of serde.
        serialized = json.dumps(json.loads(serialized))

        # Load the handle back from the dict.
        handle = serve_handle_from_json_dict(json.loads(serialized))
        assert await call(handle, "hi") == "hi"


def test_chain_of_values():
    with InputNode() as dag_input:
        out = fn.bind(1)
        out_2 = fn.bind(out, incr=2)
        out_val = fn.bind(out_2, incr=3)
        model = Model.bind(out_val)
        ray_dag = model.forward.bind(dag_input)

    json_serialized = json.dumps(ray_dag, cls=DAGNodeEncoder)
    deserialized_dag_node = json.loads(json_serialized, object_hook=dagnode_from_json)

    assert ray.get(deserialized_dag_node.execute(2)) == ray.get(ray_dag.execute(2))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
