import pytest
import json
from typing import TypeVar

import ray
from ray.serve.pipeline.json_serde import dagnode_to_json, dagnode_from_json
from ray.serve.pipeline.tests.test_modules import (
    Model,
    combine,
    ClassHello,
    fn_hello,
)


RayHandleLike = TypeVar("RayHandleLike")


# def test_simple_function_node_json_serde(shared_ray_instance):
#     """
#     Test the following behavior
#         1) Ray DAG node can go through full JSON serde cycle
#         2) Ray DAG node and deserialized DAG node produces same output

#     Against following test cases
#         - Simple function with no args
#         - Simple function with only args, all primitive types
#         - Simple function with args + kwargs, all primitive types
#     """
#     original_dag_node = combine._bind(1, 2)
#     json_serialized = dagnode_to_json(original_dag_node)
#     assert json_serialized == {
#         "type": "FunctionNode",
#         "import_path": "ray.serve.pipeline.tests.test_modules.combine",
#         "kwargs": "{}",
#         "args": "[1, 2]",
#     }
#     deserialized_dag_node = dagnode_from_json(json_serialized)
#     assert ray.get(deserialized_dag_node.execute()) == ray.get(
#         original_dag_node.execute()
#     )

#     original_dag_node = combine._bind(1, 2, kwargs_output=3)
#     deserialized_dag_node = dagnode_from_json(json_serialized)
#     assert deserialized_dag_node == {
#         "type": "FunctionNode",
#         "import_path": "ray.serve.pipeline.tests.test_modules.combine",
#         "kwargs": '{"kwargs_output": 3}',
#         "args": "[1, 2]",
#     }
#     assert ray.get(deserialized_dag_node.execute()) == ray.get(
#         original_dag_node.execute()
#     )

#     original_dag_node = fn_hello._bind()
#     deserialized_dag_node = dagnode_from_json(json_serialized)
#     assert deserialized_dag_node == {
#         "type": "FunctionNode",
#         "import_path": "ray.serve.pipeline.tests.test_modules.fn_hello",
#         "args": "[]",
#         "kwargs": "{}",
#     }
#     assert ray.get(deserialized_dag_node.execute()) == ray.get(
#         original_dag_node.execute()
#     )


def test_simple_class_node_json_serde(shared_ray_instance):
    """
    Test the following behavior
        1) Ray DAG node can go through full JSON serde cycle
        2) Ray DAG node and deserialized DAG node produces same output

    Against following test cases
        - Simple class with no args
        - Simple class with only args, all primitive typess
        - Simple class with args + kwargs, all primitive types
    """
    original_dag_node = ClassHello._bind()
    json_serialized = dagnode_to_json(original_dag_node)
    assert json_serialized == {
        "type": "ClassNode",
        "import_path": "ray.serve.pipeline.tests.test_modules.ClassHello",
        "args": "[]",
        "kwargs": "{}",
    }
    deserialized_dag_node = dagnode_from_json(json_serialized)
    # Creates actor of ClassHello
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.hello.remote()) == ray.get(
        deserialized_actor.hello.remote()
    )

    original_dag_node = Model._bind(1)
    json_serialized = dagnode_to_json(original_dag_node)
    assert json_serialized == {
        "type": "ClassNode",
        "import_path": "ray.serve.pipeline.tests.test_modules.Model",
        "args": "[1]",
        "kwargs": "{}",
    }
    deserialized_dag_node = dagnode_from_json(json_serialized)
    # Creates actor of Model
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.forward.remote(2)) == ray.get(
        deserialized_actor.forward.remote(2)
    )

    original_dag_node = Model._bind(1, ratio=0.5)
    json_serialized = dagnode_to_json(original_dag_node)
    assert json_serialized == {
        "type": "ClassNode",
        "import_path": "ray.serve.pipeline.tests.test_modules.Model",
        "args": "[1]",
        "kwargs": '{"ratio": 0.5}',
    }
    deserialized_dag_node = dagnode_from_json(json_serialized)
    # Creates actor of Model
    original_actor = original_dag_node.execute()
    deserialized_actor = deserialized_dag_node.execute()

    assert ray.get(original_actor.forward.remote(2)) == ray.get(
        deserialized_actor.forward.remote(2)
    )


def test_simple_class_method_node_json_serde(shared_ray_instance):
    """ """
    pass


def test_simple_deployment_node_json_serde(shared_ray_instance):
    """ """
    pass


def test_simple_deployment_method_node_json_serde(shared_ray_instance):
    """ """
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
