import copy
import logging
import operator
import threading
import time
import traceback
from typing import Any, Dict, Optional

from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler._private.util import hash_launch_conf
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.tags import (
    NODE_KIND_WORKER,
    STATUS_UNINITIALIZED,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

logger = logging.getLogger(__name__)


class BaseNodeLauncher:
    """Launches Ray nodes in the main thread using
    `BaseNodeLauncher.launch_node()`.

    This is a superclass of NodeLauncher, which launches nodes asynchronously
    in the background.

    By default, the subclass NodeLauncher is used to launch nodes in subthreads.
    That behavior can be flagged off in the provider config by setting
    `foreground_node_launch: True`; the autoscaler will then makes blocking calls to
    BaseNodeLauncher.launch_node() in the main thread.
    """

    def __init__(
        self,
        provider,
        pending,
        event_summarizer,
        node_provider_availability_tracker: NodeProviderAvailabilityTracker,
        session_name: Optional[str] = None,
        prom_metrics=None,
        node_types=None,
        index=None,
        *args,
        **kwargs,
    ):
        self.pending = pending
        self.event_summarizer = event_summarizer
        self.node_provider_availability_tracker = node_provider_availability_tracker
        self.prom_metrics = prom_metrics or AutoscalerPrometheusMetrics(
            session_name=session_name
        )
        self.provider = provider
        self.node_types = node_types
        self.index = str(index) if index is not None else ""

    def launch_node(self, config: Dict[str, Any], count: int, node_type: str):
        self.log("Got {} nodes to launch.".format(count))
        self._launch_node(config, count, node_type)
        self.pending.dec(node_type, count)
        self.prom_metrics.pending_nodes.set(self.pending.value)

    def _launch_node(self, config: Dict[str, Any], count: int, node_type: str):
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

        node_launch_start_time = time.time()

        error_msg = None
        full_exception = None
        try:
            self.provider.create_node_with_resources(
                node_config, node_tags, count, resources
            )
        except NodeLaunchException as node_launch_exception:
            self.node_provider_availability_tracker.update_node_availability(
                node_type, int(node_launch_start_time), node_launch_exception
            )

            if node_launch_exception.src_exc_info is not None:
                full_exception = "\n".join(
                    traceback.format_exception(*node_launch_exception.src_exc_info)
                )

            error_msg = (
                f"Failed to launch {{}} node(s) of type {node_type}. "
                f"({node_launch_exception.category}): "
                f"{node_launch_exception.description}"
            )
        except Exception:
            error_msg = f"Failed to launch {{}} node(s) of type {node_type}."
            full_exception = traceback.format_exc()
        else:
            # Record some metrics/observability information when a node is launched.
            launch_time = time.time() - node_launch_start_time
            for _ in range(count):
                # Note: when launching multiple nodes we observe the time it
                # took all nodes to launch for each node. For example, if 4
                # nodes were created in 25 seconds, we would observe the 25
                # second create time 4 times.
                self.prom_metrics.worker_create_node_time.observe(launch_time)
            self.prom_metrics.started_nodes.inc(count)
            self.node_provider_availability_tracker.update_node_availability(
                node_type=node_type,
                timestamp=int(node_launch_start_time),
                node_launch_exception=None,
            )

        if error_msg is not None:
            self.event_summarizer.add(
                error_msg,
                quantity=count,
                aggregate=operator.add,
            )
            self.log(error_msg)
            self.prom_metrics.node_launch_exceptions.inc()
            self.prom_metrics.failed_create_nodes.inc(count)
        else:
            self.log("Launching {} nodes, type {}.".format(count, node_type))
            self.event_summarizer.add(
                "Adding {} node(s) of type " + str(node_type) + ".",
                quantity=count,
                aggregate=operator.add,
            )

        if full_exception is not None:
            self.log(full_exception)

    def log(self, statement):
        # launcher_class is "BaseNodeLauncher", or "NodeLauncher" if called
        # from that subclass.
        launcher_class: str = type(self).__name__
        prefix = "{}{}:".format(launcher_class, self.index)
        logger.info(prefix + " {}".format(statement))


class NodeLauncher(BaseNodeLauncher, threading.Thread):
    """Launches nodes asynchronously in the background."""

    def __init__(
        self,
        provider,
        queue,
        pending,
        event_summarizer,
        node_provider_availability_tracker,
        session_name: Optional[str] = None,
        prom_metrics=None,
        node_types=None,
        index=None,
        *thread_args,
        **thread_kwargs,
    ):
        self.queue = queue
        BaseNodeLauncher.__init__(
            self,
            provider=provider,
            pending=pending,
            event_summarizer=event_summarizer,
            session_name=session_name,
            node_provider_availability_tracker=node_provider_availability_tracker,
            prom_metrics=prom_metrics,
            node_types=node_types,
            index=index,
        )
        threading.Thread.__init__(self, *thread_args, **thread_kwargs)

    def run(self):
        """Collects launch data from queue populated by StandardAutoscaler.
        Launches nodes in a background thread.

        Overrides threading.Thread.run().
        NodeLauncher.start() executes this loop in a background thread.
        """
        while True:
            config, count, node_type = self.queue.get()
            # launch_node is implemented in BaseNodeLauncher
            self.launch_node(config, count, node_type)
