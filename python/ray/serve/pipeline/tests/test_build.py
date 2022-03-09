import requests
import json

from ray.serve.pipeline.api import build


from ray.serve.pipeline.tests.resources.test_dags import (
    get_simple_func_dag,
    get_simple_class_with_class_method_dag,
    get_func_class_with_class_method_dag,
    get_multi_instantiation_class_deployment_in_init_args_dag,
    get_shared_deployment_handle_dag,
    get_multi_instantiation_class_nested_deployment_arg_dag,
)


def test_build_simple_func_dag(serve_instance):
    ray_dag, _ = get_simple_func_dag()

    deployments = build(ray_dag)
    for deployment in deployments:
        deployment.deploy()

    for _ in range(5):
        resp = requests.get(
            "http://127.0.0.1:8000/ingress", data=json.dumps([1, 2])
        )
        assert resp.text == "4"


def test_build_simple_class_with_class_method_dag(serve_instance):
    ray_dag, _ = get_simple_class_with_class_method_dag()

    deployments = build(ray_dag)
    for deployment in deployments:
        deployment.deploy()

    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "0.6"


def test_build_func_class_with_class_method_dag(serve_instance):
    ray_dag, _ = get_func_class_with_class_method_dag()
    deployments = build(ray_dag)
    for deployment in deployments:
        deployment.deploy()

    for _ in range(5):
        resp = requests.get(
            "http://127.0.0.1:8000/ingress", data=json.dumps([1, 2, 3])
        )
        assert resp.text == "8"


def test_build_multi_instantiation_class_deployment_in_init_args(
    serve_instance,
):
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
