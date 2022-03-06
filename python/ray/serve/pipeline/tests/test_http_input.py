import pytest

import ray
from ray import serve
from ray.experimental.dag.input_node import InputNode
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode
from ray.serve.api import Deployment
from ray.serve.config import DeploymentConfig
from ray.serve.pipeline.deployment_node import (
    DeploymentNode,
    DeploymentMethodNode,
)
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
)
from ray.serve.pipeline.constants import USE_SYNC_HANDLE_KEY
from ray.serve.pipeline.tests.test_modules import (
    Model,
    combine,
    NESTED_HANDLE_KEY,
)

def test_user_raw_http(serve_instance):
    with PipelineInputNode() as INPUT:
        ray_dag = combine._bind(INPUT.a, INPUT.b, INPUT.c)

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()

    assert ray.get(ray_dag.execute(a=1, b=2, c=3)) == 6
