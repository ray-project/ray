import copy
import json
import logging
import os
import subprocess
import tempfile
import time
import sys

import yaml

import ray
import ray._private.services
from ray.util.annotations import DeveloperAPI
from ray.autoscaler._private.spark.node_provider import RAY_ON_SPARK_HEAD_NODE_ID
import ray._private.ray_constants as ray_constants


logger = logging.getLogger(__name__)


@DeveloperAPI
class AutoscalingCluster:
    """Create a ray on spark autoscaling cluster.
    """

    def __init__(self, head_resources: dict, worker_node_types: dict, extra_provider_config: dict):
        """Create the cluster.

        Args:
            head_resources: resources of the head node, including CPU.
            worker_node_types: autoscaler node types config for worker nodes.
        """
        self._head_resources = head_resources.copy()
        self._head_resources["NODE_ID_AS_RESOURCE"] = RAY_ON_SPARK_HEAD_NODE_ID
        self._config = self._generate_config(
            head_resources, worker_node_types, extra_provider_config
        )

    def _generate_config(self, head_resources, worker_node_types, extra_provider_config):
        base_config = yaml.safe_load(
            open(
                os.path.join(
                    os.path.dirname(ray.__file__),
                    "autoscaler/spark/defaults.yaml",
                )
            )
        )
        custom_config = copy.deepcopy(base_config)
        custom_config["available_node_types"] = worker_node_types
        custom_config["available_node_types"]["ray.head.default"] = {
            "resources": head_resources,
            "node_config": {},
            "max_workers": 0,
        }
        custom_config["provider"].update(extra_provider_config)
        return custom_config

    def start(
        self,
        ray_head_ip,
        ray_head_port,
        ray_temp_dir,
        dashboard_options,
        head_node_options,
        collect_log_to_path,
    ):
        """Start the cluster.

        After this call returns, you can connect to the cluster with
        ray.init("auto").
        """
        from ray.util.spark.cluster_init import (
            _convert_ray_node_options,
            exec_cmd,
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH,
            _append_resources_config,
        )

        _, autoscale_config = tempfile.mkstemp()
        with open(autoscale_config, "w") as f:
            f.write(json.dumps(self._config))

        ray_head_node_cmd = [
            sys.executable,
            "-m",
            "ray.util.spark.start_ray_node",
            f"--temp-dir={ray_temp_dir}",
            "--block",
            "--head",
            f"--node-ip-address={ray_head_ip}",
            f"--port={ray_head_port}",
            f"--autoscaling-config={autoscale_config}",
            *dashboard_options,
        ]

        if "CPU" in self._head_resources:
            ray_head_node_cmd.append("--num-cpus={}".format(self._head_resources.pop("CPU")))
        if "GPU" in self._head_resources:
            ray_head_node_cmd.append("--num-gpus={}".format(self._head_resources.pop("GPU")))
        if "memory" in self._head_resources:
            ray_head_node_cmd.append("--memory={}".format(self._head_resources.pop("memory")))
        if "object_store_memory" in self._head_resources:
            ray_head_node_cmd.append("--object-store-memory={}".format(self._head_resources.pop("object_store_memory")))

        head_node_options = _append_resources_config(head_node_options, self._head_resources)
        ray_head_node_cmd.extend(_convert_ray_node_options(head_node_options))

        extra_env = {
            "AUTOSCALER_UPDATE_INTERVAL_S": "1",
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or "",
        }

        self.ray_head_node_cmd = ray_head_node_cmd
        
        return exec_cmd(
            ray_head_node_cmd,
            synchronous=False,
            extra_env=extra_env,
        )
