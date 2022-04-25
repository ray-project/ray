from typing import Any, Optional, Dict
import copy
import logging
import operator
import threading
import traceback
import time

from ray.autoscaler.tags import (
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_USER_NODE_TYPE,
    STATUS_UNINITIALIZED,
    NODE_KIND_WORKER,
)
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler._private.util import hash_launch_conf

logger = logging.getLogger(__name__)


class NodeLauncher(threading.Thread):
    """Launches nodes asynchronously in the background."""

    def __init__(
        self,
        provider,
        queue,
        pending,
        event_summarizer,
        prom_metrics=None,
        node_types=None,
        index=None,
        *args,
        **kwargs,
    ):
        self.queue = queue
        self.pending = pending
        self.prom_metrics = prom_metrics or AutoscalerPrometheusMetrics()
        self.provider = provider
        self.node_types = node_types
        self.index = str(index) if index is not None else ""
        self.event_summarizer = event_summarizer
        super(NodeLauncher, self).__init__(*args, **kwargs)

    def _launch_node(
        self, config: Dict[str, Any], count: int, node_type: Optional[str]
    ):
        if self.node_types:
            assert node_type, node_type

        # The `worker_nodes` field is deprecated in favor of per-node-type
        # node_configs. We allow it for backwards-compatibility.
        launch_config = copy.deepcopy(config.get("worker_nodes", {}))
        if node_type:
            launch_config.update(
                config["available_node_types"][node_type]["node_config"]
            )
        resources = copy.deepcopy(
            config["available_node_types"][node_type]["resources"]
        )
        launch_hash = hash_launch_conf(launch_config, config["auth"])
        self.log("Launching {} nodes, type {}.".format(count, node_type))
        node_config = copy.deepcopy(config.get("worker_nodes", {}))
        node_tags = {
            TAG_RAY_NODE_NAME: "ray-{}-worker".format(config["cluster_name"]),
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
            TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
            TAG_RAY_LAUNCH_CONFIG: launch_hash,
        }
        # A custom node type is specified; set the tag in this case, and also
        # merge the configs. We merge the configs instead of overriding, so
        # that the bootstrapped per-cloud properties are preserved.
        # TODO(ekl) this logic is duplicated in commands.py (keep in sync)
        if node_type:
            node_tags[TAG_RAY_USER_NODE_TYPE] = node_type
            node_config.update(launch_config)
        launch_start_time = time.time()
        self.provider.create_node_with_resources(
            node_config, node_tags, count, resources
        )
        launch_time = time.time() - launch_start_time
        for _ in range(count):
            # Note: when launching multiple nodes we observe the time it
            # took all nodes to launch for each node. For example, if 4
            # nodes were created in 25 seconds, we would observe the 25
            # second create time 4 times.
            self.prom_metrics.worker_create_node_time.observe(launch_time)
        self.prom_metrics.started_nodes.inc(count)

    def run(self):
        while True:
            config, count, node_type = self.queue.get()
            self.log("Got {} nodes to launch.".format(count))
            try:
                self._launch_node(config, count, node_type)
            except Exception:
                self.prom_metrics.node_launch_exceptions.inc()
                self.prom_metrics.failed_create_nodes.inc(count)
                self.event_summarizer.add(
                    "Failed to launch {} nodes of type " + node_type + ".",
                    quantity=count,
                    aggregate=operator.add,
                )
                # Log traceback from failed node creation only once per minute
                # to avoid spamming driver logs with tracebacks.
                self.event_summarizer.add_once_per_interval(
                    message="Node creation failed. See the traceback below."
                    " See autoscaler logs for further details.\n"
                    f"{traceback.format_exc()}",
                    key="Failed to create node.",
                    interval_s=60,
                )
                logger.exception("Launch failed")
            finally:
                self.pending.dec(node_type, count)
                self.prom_metrics.pending_nodes.set(self.pending.value)

    def log(self, statement):
        prefix = "NodeLauncher{}:".format(self.index)
        logger.info(prefix + " {}".format(statement))
