import copy
import json
import logging
import os
import subprocess
import tempfile
import time

import yaml

import ray
import ray._private.services
from ray.util.annotations import DeveloperAPI
from ray.autoscaler._private.spark.node_provider import RAY_ON_SPARK_HEAD_NODE_ID

logger = logging.getLogger(__name__)


@DeveloperAPI
class AutoscalingCluster:
    """Create a ray on spark autoscaling cluster.
    """

    def __init__(self, head_resources: dict, worker_node_types: dict, **config_kwargs):
        """Create the cluster.

        Args:
            head_resources: resources of the head node, including CPU.
            worker_node_types: autoscaler node types config for worker nodes.
        """
        self._head_resources = head_resources
        self._config = self._generate_config(
            head_resources, worker_node_types, **config_kwargs
        )

    def _generate_config(self, head_resources, worker_node_types, **config_kwargs):
        base_config = yaml.safe_load(
            open(
                os.path.join(
                    os.path.dirname(ray.__file__),
                    "autoscaler/spark/example.yaml",
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
        custom_config.update(config_kwargs)
        return custom_config

    def start(self, _system_config=None):
        """Start the cluster.

        After this call returns, you can connect to the cluster with
        ray.init("auto").
        """
        _, fake_config = tempfile.mkstemp()
        with open(fake_config, "w") as f:
            f.write(json.dumps(self._config))
        cmd = [
            "ray",
            "start",
            "--autoscaling-config={}".format(fake_config),
            "--head",
        ]
        if "CPU" in self._head_resources:
            cmd.append("--num-cpus={}".format(self._head_resources.pop("CPU")))
        if "GPU" in self._head_resources:
            cmd.append("--num-gpus={}".format(self._head_resources.pop("GPU")))
        if self._head_resources:
            cmd.append("--resources='{}'".format(json.dumps(self._head_resources)))
        if _system_config is not None:
            cmd.append(
                "--system-config={}".format(
                    json.dumps(_system_config, separators=(",", ":"))
                )
            )
        env = os.environ.copy()
        env.update({
            "AUTOSCALER_UPDATE_INTERVAL_S": "1",
            "RAY_OVERRIDE_NODE_ID_FOR_TESTING": RAY_ON_SPARK_HEAD_NODE_ID,
        })
        subprocess.check_call(cmd, env=env)

    def shutdown(self):
        """Terminate the cluster."""
        subprocess.check_call(["ray", "stop", "--force"])
