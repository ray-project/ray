from typing import Any, Dict, List, OrderedDict
import uuid
import threading
import json
from importlib_metadata import version
import yaml

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    FunctionNode,
)
from ray.serve.api import Deployment, DeploymentConfig
from ray import serve
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.deployment_node import DeploymentNode
from ray.serve.pipeline.json_serde import DAGNodeEncoder


class DeploymentIDGenerator(object):
    """
    Generate unique suffix for each given deployment_name requested for id.
    By default uses deployment_name for the very first time, then append
    monotonic increasing id to it.
    """

    __lock = threading.Lock()
    __shared_state = dict()

    @classmethod
    def get_deployment_id(cls, deployment_name: str):
        with cls.__lock:
            if deployment_name not in cls.__shared_state:
                cls.__shared_state[deployment_name] = 0
                return deployment_name
            else:
                suffix_num = cls.__shared_state[deployment_name] + 1
                cls.__shared_state[deployment_name] = suffix_num

                return f"{deployment_name}_{suffix_num}"


def transform_ray_dag_to_serve_dag(dag_node, deployments=None):
    if isinstance(dag_node, ClassNode):
        deployment_name = (
            dag_node.get_options().get("name", None) or dag_node._body.__name__
        )
        deployment_name = DeploymentIDGenerator.get_deployment_id(
            deployment_name
        )
        # Clean up keys with default value
        ray_actor_options = {
            k: v for k, v in dag_node.get_options().items() if v
        }
        if ray_actor_options.get("placement_group") == "default":
            del ray_actor_options["placement_group"]
        if ray_actor_options.get("placement_group_bundle_index") == -1:
            del ray_actor_options["placement_group_bundle_index"]
        if ray_actor_options.get("max_pending_calls") == -1:
            del ray_actor_options["max_pending_calls"]

        args = dag_node.get_args()
        init_args = []
        for arg in args:
            if isinstance(arg, DeploymentNode):
                init_args.append(arg._deployment_handle)
            else:
                init_args.append(arg)
        # Deployment class cannot bind with DeploymentNode
        new_deployment = Deployment(
            dag_node._body,
            deployment_name,
            DeploymentConfig(),
            init_args=tuple(init_args),  # replace DeploymentNode with handle
            init_kwargs=dag_node.get_kwargs(),
            ray_actor_options=ray_actor_options,
            _internal=True,
        )
        deployment_node = DeploymentNode(
            new_deployment,
            deployment_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            ray_actor_options,
            other_args_to_resolve={},
        )
        if deployments:
            deployments.append(new_deployment)

        return deployment_node
    elif isinstance(dag_node, ClassMethodNode):
        other_args_to_resolve = dag_node.get_other_args_to_resolve()
        parent_deployment_node = other_args_to_resolve["parent_class_node"]

        deployment_method_node = DeploymentMethodNode(
            parent_deployment_node._body,
            parent_deployment_node._deployment_name,
            dag_node._method_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            dag_node.get_options(),
        )
        return deployment_method_node
    else:
        return dag_node


def extract_deployments_from_serve_dag(
    serve_dag_root: DAGNode,
) -> List[Deployment]:
    deployments = []

    def extractor(dag_node):
        if isinstance(dag_node, DeploymentMethodNode):
            deployments.append(dag_node._body)

    serve_dag_root._apply_recursive(lambda node: extractor(node))

    return deployments


def get_dag_runner_deployment(serve_dag_root, pipeline_root_name=None):
    pipeline_root_name = (
        pipeline_root_name or f"serve_pipeline_root_{uuid.uuid4().hex}"
    )
    serve_dag_root_json = json.dumps(serve_dag_root, cls=DAGNodeEncoder)

    serve_dag_root_deployment = serve.deployment(
        name="pipeline_root_name",
    )("ray.serve.pipeline.dag_runner.DAGRunner")
    serve_dag_root_deployment._init_args = (serve_dag_root_json,)

    return serve_dag_root_deployment


def build_yaml(deployments: List[Deployment]) -> str:
    """
    Given a serve transformed and extracted list of deployments, generate
    a build file from it.

    Sample yaml file:

    deployments:
      - name: shallow
        version: None
        prev_version: None
        init_args: None
        init_kwargs: None
        import_path: "test_env.shallow_import.ShallowClass"
        configurable:
            num_replicas: 3
            route_prefix: None
            ray_actor_options:
                runtime_env:
                py_modules:
                    - "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
                    - "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
            user_config: None
            max_concurrent_queries: None
            _autoscaling_config: None
            _graceful_shutdown_wait_loop_s: None
            _graceful_shutdown_timeout_s: None
    """
    deployments_data = []
    for d in deployments:
        assert isinstance(d._func_or_class, str), (
            "Deployment body for build_yaml() must be an import_path as "
            f"string, got type: {type(d._func_or_class)}"
        )

        deployments_data.append(
            dict(
                name=d.name,
                version=d.version,
                prev_version=d.prev_version,
                init_args=d.init_args,
                init_kwargs=d.init_kwargs,
                import_path=d._func_or_class,
                configurable=dict(
                    num_replicas=d.num_replicas,
                    route_prefix=d.route_prefix,
                    ray_actor_optoins=d._ray_actor_options,
                    user_config=d.user_config,
                    max_concurrent_queries=d.max_concurrent_queries,
                    _autoscaling_config=d._config.autoscaling_config,
                    _graceful_shutdown_wait_loop_s=d._config.graceful_shutdown_wait_loop_s,
                    _graceful_shutdown_timeout_s=d._config.graceful_shutdown_timeout_s,
                ),
            ),
        )

    # requires python >= 3.6
    return yaml.safe_dump(deployments_data, sort_keys=False)
