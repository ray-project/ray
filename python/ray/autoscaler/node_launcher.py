import threading
import logging

from ray.autoscaler.tags import (TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_STATUS,
                                 TAG_RAY_NODE_TYPE, TAG_RAY_NODE_NAME,
                                 STATUS_UNINITIALIZED, NODE_TYPE_WORKER)
from ray.autoscaler.util import hash_launch_conf

logger = logging.getLogger(__name__)


class NodeLauncher(threading.Thread):
    """Launches a node asynchronously in a thread."""

    def __init__(self, provider, queue, pending, index=None, *args, **kwargs):
        self.queue = queue
        self.pending = pending
        self.provider = provider
        self.index = str(index) if index is not None else ""
        super(NodeLauncher, self).__init__(*args, **kwargs)

    def _launch_node(self, config, count):
        worker_filter = {TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER}
        before = self.provider.non_terminated_nodes(tag_filters=worker_filter)
        launch_hash = hash_launch_conf(config["worker_nodes"], config["auth"])
        self.log("Launching {} nodes.".format(count))
        node_config = copy.deepcopy(config["worker_nodes"])
        self.provider.create_node(
            node_config, {
                TAG_RAY_NODE_NAME: "ray-{}-worker".format(
                    config["cluster_name"]),
                TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER,
                TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
                TAG_RAY_LAUNCH_CONFIG: launch_hash,
            }, count)
        after = self.provider.non_terminated_nodes(tag_filters=worker_filter)
        if set(after).issubset(before):
            self.log("No new nodes reported after node creation.")

    def run(self):
        while True:
            config, count = self.queue.get()
            self.log("Got {} nodes to launch.".format(count))
            try:
                self._launch_node(config, count)
            except Exception:
                logger.exception("Launch failed")
            finally:
                self.pending.dec(count)

    def log(self, statement):
        prefix = "NodeLauncher{}:".format(self.index)
        logger.info(prefix + " {}".format(statement))
