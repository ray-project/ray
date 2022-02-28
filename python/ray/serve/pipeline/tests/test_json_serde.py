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
    ClassHello,
    fn_hello,
)
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
)
from ray.experimental.dag import InputNode
from ray import serve

RayHandleLike = TypeVar("RayHandleLike")


def test_simple_function_node_json_serde(serve_instance):
    """
    Test the following behavior
        1) Ray DAG node can go through full JSON serde cycle
        2) Ray DAG node and deserialized DAG node produces same output
    Against following test cases
        - Simple function with no args
        - Simple function with only args, all primitive types
        - Simple function with args + kwargs, all primitive types
    """
    original_dag_node = combine._bind(1, 2)
    json_serialized = json.dumps(original_dag_node, cls=DAGNodeEncoder)
    assert json_serialized == json.dumps(
        {
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.combine",
            "args": "[1, 2]",
            "kwargs": "{}",
        }
    )
    deserialized_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    assert ray.get(deserialized_dag_node.execute()) == ray.get(
        original_dag_node.execute()
    )

    original_dag_node = combine._bind(1, 2, kwargs_output=3)
    json_serialized = json.dumps(original_dag_node, cls=DAGNodeEncoder)
    assert json_serialized == json.dumps(
        {
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.combine",
            "args": "[1, 2]",
            "kwargs": '{"kwargs_output": 3}',
        }
    )
    deserialized_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    assert ray.get(deserialized_dag_node.execute()) == ray.get(
        original_dag_node.execute()
    )

    original_dag_node = fn_hello._bind()
    json_serialized = json.dumps(original_dag_node, cls=DAGNodeEncoder)
    assert json_serialized == json.dumps(
        {
            DAGNODE_TYPE_KEY: "FunctionNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.fn_hello",
            "args": "[]",
            "kwargs": "{}",
        }
    )
    deserialized_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    assert ray.get(deserialized_dag_node.execute()) == ray.get(
        original_dag_node.execute()
    )


def test_simple_class_node_json_serde(serve_instance):
    """
    Test the following behavior
        1) Ray DAG node can go through full JSON serde cycle
        2) Ray DAG node and deserialized DAG node produces same actor instances
            with same method call output
    Against following test cases
        - Simple class with no args
        - Simple class with only args, all primitive typess
        - Simple class with args + kwargs, all primitive types
    """
    original_dag_node = ClassHello._bind()
    json_serialized = json.dumps(original_dag_node, cls=DAGNodeEncoder)
    assert json_serialized == json.dumps(
        {
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.ClassHello",
            "args": "[]",
            "kwargs": "{}",
        }
    )
    deserialized_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    # Creates actor of ClassHello
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.hello.remote()) == ray.get(
        deserialized_actor.hello.remote()
    )

    original_dag_node = Model._bind(1)
    json_serialized = json.dumps(original_dag_node, cls=DAGNodeEncoder)
    assert json_serialized == json.dumps(
        {
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.Model",
            "args": "[1]",
            "kwargs": "{}",
        }
    )
    deserialized_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    # Creates actor of Model
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.forward.remote(2)) == ray.get(
        deserialized_actor.forward.remote(2)
    )

    original_dag_node = Model._bind(1, ratio=0.5)
    json_serialized = json.dumps(original_dag_node, cls=DAGNodeEncoder)
    assert json_serialized == json.dumps(
        {
            DAGNODE_TYPE_KEY: "ClassNode",
            "import_path": "ray.serve.pipeline.tests.test_modules.Model",
            "args": "[1]",
            "kwargs": '{"ratio": 0.5}',
        }
    )
    deserialized_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    # Creates actor of Model
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.forward.remote(2)) == ray.get(
        deserialized_actor.forward.remote(2)
    )


def test_nested_deployment_node_json_serde(serve_instance):
    # Ray DAG
    """
    (FunctionNode)(
        body=ray.serve.pipeline.tests.test_modules.combine
        args=[
            (ClassMethodNode)(
                body=ray.serve.pipeline.tests.test_modules.Model.forward()
                args=[
                    (InputNode)(
                        body=__InputNode__
                        args=[]
                        kwargs={}
                        options={}
                        other_args_to_resolve={}
                    )
                ]
                kwargs={}
                options={}
                other_args_to_resolve={
                    parent_class_node:
                        (ClassNode)(
                            body=ray.serve.pipeline.tests.test_modules.Model
                            args=[
                                2,
                            ]
                            kwargs={}
                            options={}
                            other_args_to_resolve={}
                        )
                    prev_class_method_call: None
                }
            )
            (ClassMethodNode)(
                body=ray.serve.pipeline.tests.test_modules.Model.forward()
                args=[
                    (InputNode)(
                        body=__InputNode__
                        args=[]
                        kwargs={}
                        options={}
                        other_args_to_resolve={}
                    )
                ]
                kwargs={}
                options={}
                other_args_to_resolve={
                    parent_class_node:
                        (ClassNode)(
                            body=ray.serve.pipeline.tests.test_modules.Model
                            args=[
                                3,
                            ]
                            kwargs={}
                            options={}
                            other_args_to_resolve={}
                        )
                    prev_class_method_call: None
                }
            )
        ]
        kwargs={}
        options={}
        other_args_to_resolve={}
    )
    """
    # Transformed serve dag
    """
    (FunctionNode)(
        body=ray.serve.pipeline.tests.test_modules.combine
        args=[
            (DeploymentMethodNode)(
                method=forward()
                deployment=Deployment(name=Model,version=None,route_prefix=/Model)
                args=[
                    (InputNode)(
                        body=__InputNode__
                        args=[]
                        kwargs={}
                        options={}
                        other_args_to_resolve={}
                    )
                ]
                kwargs={}
                options={}
                other_args_to_resolve={}
            )
            (DeploymentMethodNode)(
                method=forward()
                deployment=Deployment(name=Model,version=None,route_prefix=/Model)
                args=[
                    (InputNode)(
                        body=__InputNode__
                        args=[]
                        kwargs={}
                        options={}
                        other_args_to_resolve={}
                    )
                ]
                kwargs={}
                options={}
                other_args_to_resolve={}
            )
        ]
        kwargs={}
        options={}
        other_args_to_resolve={}
    )
    """

    m1 = Model._bind(2)
    m2 = Model._bind(3)

    m1_output = m1.forward._bind(InputNode())
    m2_output = m2.forward._bind(InputNode())

    ray_dag = combine._bind(m1_output, m2_output)
    original_serve_root_dag = ray_dag._apply_recursive(
        lambda node: transform_ray_dag_to_serve_dag(node)
    )
    json_serialized = json.dumps(
        original_serve_root_dag, indent=4, cls=DAGNodeEncoder
    )
    deserialized_serve_root_dag_node = json.loads(
        json_serialized, object_hook=dagnode_from_json
    )
    # Deployment init_arg is (2,)
    original_deployments = extract_deployments_from_serve_dag(
        original_serve_root_dag
    )
    # Deployment init_arg is [InputNode..]
    deserialized_deployments = extract_deployments_from_serve_dag(
        deserialized_serve_root_dag_node
    )
    # Deploy deserilized version to ensure JSON serde correctness
    for model in deserialized_deployments:
        model.deploy()

    assert ray.get(ray_dag.execute(1)) == ray.get(
        original_serve_root_dag.execute(1)
    )
    assert ray.get(ray_dag.execute(1)) == ray.get(
        deserialized_serve_root_dag_node.execute(1)
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))