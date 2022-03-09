import pytest
import requests

import ray

from ray.serve.pipeline.api import build

from ray.serve.pipeline.tests.resources.test_modules import (
    request_to_data_int,
)
from ray.serve.pipeline.tests.resources.test_dags import (
    get_multi_instantiation_class_deployment_in_init_args_dag,
    get_shared_deployment_handle_dag,
    get_multi_instantiation_class_nested_deployment_arg_dag,
)
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode


def test_no_inline_class_or_func(serve_instance):
    # This function is defined inline given it lives in the test function
    @ray.remote
    def inline_func(val):
        return val

    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        ray_dag = inline_func._bind(dag_input)

    assert ray.get(ray_dag.execute(1)) == 1
    with pytest.raises(
        AssertionError,
        match="Function used in DAG should not be in-line defined",
    ):
        _ = build(ray_dag)

    # This class is defined inline given it lives in the test function
    @ray.remote
    class InlineClass:
        def __init__(self, val):
            self.val = val

        def get(self, input):
            return self.val + input

    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        node = InlineClass._bind(1)
        ray_dag = node.get._bind(dag_input)

    with pytest.raises(
        AssertionError,
        match="Class used in DAG should not be in-line defined",
    ):
        _ = build(ray_dag)


def test_build_multi_instantiation_class_deployment_in_init_args(serve_instance):
    """
    Test we can pass deployments as init_arg or init_kwarg, instantiated
    multiple times for the same class, and we can still correctly replace
    args with deployment handle and parse correct deployment instances.
    """
    ray_dag, _ = get_multi_instantiation_class_deployment_in_init_args_dag()

    deployments = build(ray_dag)
    for deployment in deployments:
        deployment.deploy()

    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "5"


def test_build_shared_deployment_handle(serve_instance):
    """
    Test we can re-use the same deployment handle multiple times or in
    multiple places, without incorrectly parsing duplicated deployments.
    """
    ray_dag, _ = get_shared_deployment_handle_dag()

    deployments = build(ray_dag)
    for deployment in deployments:
        deployment.deploy()

    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "4"


def test_build_multi_instantiation_class_nested_deployment_arg(serve_instance):
    """
    Test we can pass deployments with **nested** init_arg or init_kwarg,
    instantiated multiple times for the same class, and we can still correctly
    replace args with deployment handle and parse correct deployment instances.
    """
    ray_dag, _ = get_multi_instantiation_class_nested_deployment_arg_dag()

    deployments = build(ray_dag)
    for deployment in deployments:
        deployment.deploy()

    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "5"
