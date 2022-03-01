import pytest
import json
from typing import TypeVar

import ray
from ray.serve.pipeline.json_serde import (
    DAGNodeEncoder,
    dagnode_from_json,
    DAGNODE_TYPE_KEY,
)
from ray.serve.pipeline.tests.test_modules import (
    Model,
    combine,
    Counter,
    ClassHello,
    fn_hello,
    Combine,
    NESTED_HANDLE_KEY,
)
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
)
from ray.experimental.dag import InputNode

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


def _test_execution_class_method_node_Counter(original_dag_node, deserialized_dag_node):
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
    if expected_json_dict != "SKIP":
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
    original_dag_node = combine._bind(1, 2)
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_function_node,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.combine",
            "args": "[1, 2]",
            "kwargs": "{}",
            "options": "{}",
            "other_args_to_resolve": "{}",
        },
    )

    original_dag_node = combine._bind(1, 2, kwargs_output=3)
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_function_node,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.combine",
            "args": "[1, 2]",
            "kwargs": '{"kwargs_output": 3}',
            "options": "{}",
            "other_args_to_resolve": "{}",
        },
    )

    original_dag_node = fn_hello._bind()
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_function_node,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.fn_hello",
            "args": "[]",
            "kwargs": "{}",
            "options": "{}",
            "other_args_to_resolve": "{}",
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
    original_dag_node = ClassHello._bind()
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_class_node_ClassHello,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.ClassHello",
            "args": "[]",
            "kwargs": "{}",
            "options": "{}",
            "other_args_to_resolve": "{}",
        },
    )

    original_dag_node = Model._bind(1)
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_class_node_Model,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.Model",
            "args": "[1]",
            "kwargs": "{}",
            "options": "{}",
            "other_args_to_resolve": "{}",
        },
    )

    original_dag_node = Model._bind(1, ratio=0.5)
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_class_node_Model,
        expected_json_dict={
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.Model",
            "args": "[1]",
            "kwargs": '{"ratio": 0.5}',
            "options": "{}",
            "other_args_to_resolve": "{}",
        },
    )

    counter = Counter._bind(0)
    counter.inc._bind(1)
    counter.inc._bind(2)
    original_dag_node = counter.get._bind()
    print(original_dag_node)
    assert ray.get(original_dag_node.execute()) == 3
    _test_json_serde_helper(
        original_dag_node,
        executor_fn=_test_execution_class_method_node_Counter,
        # TODO: (jiaodong) Make nested ClassMethodNode JSON more human readable
        # thus feasible to manually write a test
        expected_json_dict="SKIP",
    )


def test_multi_instantiation_class_nested_deployment_arg(serve_instance):
    m1 = Model._bind(2)
    m2 = Model._bind(3)
    combine = Combine._bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
    ray_dag = combine.__call__._bind(InputNode())

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)

    json_serialized = json.dumps(serve_root_dag, cls=DAGNodeEncoder)
    deserialized_serve_root_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    deserialized_deployments = extract_deployments_from_serve_dag(
        deserialized_serve_root_dag_node
    )
    assert len(deserialized_deployments) == 3
    # Deploy deserilized version to ensure JSON serde correctness
    for model in deserialized_deployments:
        model.deploy()
    assert ray.get(ray_dag.execute(1)) == ray.get(serve_root_dag.execute(1))
    assert ray.get(ray_dag.execute(1)) == ray.get(
        deserialized_serve_root_dag_node.execute(1)
    )


def test_nested_deployment_node_json_serde(serve_instance):
    m1 = Model._bind(2)
    m2 = Model._bind(3)

    m1_output = m1.forward._bind(InputNode())
    m2_output = m2.forward._bind(InputNode())

    ray_dag = combine._bind(m1_output, m2_output)
    original_serve_root_dag = ray_dag._apply_recursive(
        lambda node: transform_ray_dag_to_serve_dag(node)
    )
    json_serialized = json.dumps(original_serve_root_dag, cls=DAGNodeEncoder)
    deserialized_serve_root_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    # Deployment init_arg is [InputNode..]
    deserialized_deployments = extract_deployments_from_serve_dag(
        deserialized_serve_root_dag_node
    )
    # Deploy deserilized version to ensure JSON serde correctness
    for model in deserialized_deployments:
        model.deploy()

    assert ray.get(ray_dag.execute(1)) == ray.get(original_serve_root_dag.execute(1))
    assert ray.get(ray_dag.execute(1)) == ray.get(
        deserialized_serve_root_dag_node.execute(1)
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
