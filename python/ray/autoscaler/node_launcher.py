from typing import Any, Optional, Dict
import copy
import logging
import threading

from ray.autoscaler.tags import (TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_STATUS,
                                 TAG_RAY_NODE_TYPE, TAG_RAY_NODE_NAME,
                                 TAG_RAY_INSTANCE_TYPE, STATUS_UNINITIALIZED,
                                 NODE_TYPE_WORKER)
from ray.autoscaler.util import hash_launch_conf

logger = logging.getLogger(__name__)


class NodeLauncher(threading.Thread):
    """Launches nodes asynchronously in the background."""

    def __init__(self,
                 provider,
                 queue,
                 pending,
                 instance_types=None,
                 index=None,
                 *args,
                 **kwargs):
        self.queue = queue
        self.pending = pending
        self.provider = provider
        self.instance_types = instance_types
        self.index = str(index) if index is not None else ""
        super(NodeLauncher, self).__init__(*args, **kwargs)

    def _launch_node(self, config: Dict[str, Any], count: int,
                     instance_type: Optional[str]):
        if self.instance_types:
            assert instance_type, instance_type
        worker_filter = {TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER}
        before = self.provider.non_terminated_nodes(tag_filters=worker_filter)
        launch_hash = hash_launch_conf(config["worker_nodes"], config["auth"])
        self.log("Launching {} nodes, type {}.".format(count, instance_type))
        node_config = copy.deepcopy(config["worker_nodes"])
        node_tags = {
            TAG_RAY_NODE_NAME: "ray-{}-worker".format(config["cluster_name"]),
            TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER,
            TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
            TAG_RAY_LAUNCH_CONFIG: launch_hash,
        }
        # A custom node type is specified; set the tag in this case, and also
        # merge the configs. We merge the configs instead of overriding, so
        # that the bootstrapped per-cloud properties are preserved.
        # TODO(ekl) this logic is duplicated in commands.py (keep in sync)
        if instance_type:
            node_tags[TAG_RAY_INSTANCE_TYPE] = instance_type
            node_config.update(
                config["available_node_types"][instance_type]["node_config"])
        self.provider.create_node(node_config, node_tags, count)
        after = self.provider.non_terminated_nodes(tag_filters=worker_filter)
        if set(after).issubset(before):
            self.log("No new nodes reported after node creation.")

    def run(self):
        while True:
            config, count, instance_type = self.queue.get()
            self.log("Got {} nodes to launch.".format(count))
            try:
                self._launch_node(config, count, instance_type)
            except Exception:
                logger.exception("Launch failed")
            finally:
                self.pending.dec(instance_type, count)

    def log(self, statement):
        prefix = "NodeLauncher{}:".format(self.index)
        logger.info(prefix + " {}".format(statement))
