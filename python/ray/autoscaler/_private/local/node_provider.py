import json
import logging
import os
import socket
from threading import RLock

from filelock import FileLock

from ray.autoscaler._private.local.config import (
    LOCAL_CLUSTER_NODE_TYPE,
    bootstrap_local,
    get_lock_path,
    get_state_path,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_UP_TO_DATE,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

logger = logging.getLogger(__name__)

logging.getLogger("filelock").setLevel(logging.WARNING)


class ClusterState:
    def __init__(self, lock_path, save_path, provider_config):
        self.lock = RLock()
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        self.file_lock = FileLock(lock_path)
        self.save_path = save_path

        with self.lock:
            with self.file_lock:
                if os.path.exists(self.save_path):
                    workers = json.loads(open(self.save_path).read())
                    head_config = workers.get(provider_config["head_ip"])
                    if (
                        not head_config
                        or head_config.get("tags", {}).get(TAG_RAY_NODE_KIND)
                        != NODE_KIND_HEAD
                    ):
                        workers = {}
                        logger.info("Head IP changed - recreating cluster.")
                else:
                    workers = {}
                logger.info(
                    "ClusterState: Loaded cluster state: {}".format(list(workers))
                )
                for worker_ip in provider_config["worker_ips"]:
                    if worker_ip not in workers:
                        workers[worker_ip] = {
                            "tags": {TAG_RAY_NODE_KIND: NODE_KIND_WORKER},
                            "state": "terminated",
                        }
                    else:
                        assert (
                            workers[worker_ip]["tags"][TAG_RAY_NODE_KIND]
                            == NODE_KIND_WORKER
                        )
                if provider_config["head_ip"] not in workers:
                    workers[provider_config["head_ip"]] = {
                        "tags": {TAG_RAY_NODE_KIND: NODE_KIND_HEAD},
                        "state": "terminated",
                    }
                else:
                    assert (
                        workers[provider_config["head_ip"]]["tags"][TAG_RAY_NODE_KIND]
                        == NODE_KIND_HEAD
                    )
                # Relevant when a user reduces the number of workers
                # without changing the headnode.
                list_of_node_ips = list(provider_config["worker_ips"])
                list_of_node_ips.append(provider_config["head_ip"])
                for worker_ip in list(workers):
                    if worker_ip not in list_of_node_ips:
                        del workers[worker_ip]

                # Set external head ip, if provided by user.
                # Necessary if calling `ray up` from outside the network.
                # Refer to LocalNodeProvider.external_ip function.
                external_head_ip = provider_config.get("external_head_ip")
                if external_head_ip:
                    head = workers[provider_config["head_ip"]]
                    head["external_ip"] = external_head_ip

                assert len(workers) == len(provider_config["worker_ips"]) + 1
                with open(self.save_path, "w") as f:
                    logger.debug(
                        "ClusterState: Writing cluster state: {}".format(workers)
                    )
                    f.write(json.dumps(workers))

    def get(self):
        with self.lock:
            with self.file_lock:
                workers = json.loads(open(self.save_path).read())
                return workers

    def put(self, worker_id, info):
        assert "tags" in info
        assert "state" in info
        with self.lock:
            with self.file_lock:
                workers = self.get()
                workers[worker_id] = info
                with open(self.save_path, "w") as f:
                    logger.info(
                        "ClusterState: "
                        "Writing cluster state: {}".format(list(workers))
                    )
                    f.write(json.dumps(workers))


class OnPremCoordinatorState(ClusterState):
    """Generates & updates the state file of CoordinatorSenderNodeProvider.

    Unlike ClusterState, which generates a cluster specific file with
    predefined head and worker ips, OnPremCoordinatorState overwrites
    ClusterState's __init__ function to generate and manage a unified
    file of the status of all the nodes for multiple clusters.
    """

    def __init__(self, lock_path, save_path, list_of_node_ips):
        self.lock = RLock()
        self.file_lock = FileLock(lock_path)
        self.save_path = save_path

        with self.lock:
            with self.file_lock:
                if os.path.exists(self.save_path):
                    nodes = json.loads(open(self.save_path).read())
                else:
                    nodes = {}
                logger.info(
                    "OnPremCoordinatorState: "
                    "Loaded on prem coordinator state: {}".format(nodes)
                )

                # Filter removed node ips.
                for node_ip in list(nodes):
                    if node_ip not in list_of_node_ips:
                        del nodes[node_ip]

                for node_ip in list_of_node_ips:
                    if node_ip not in nodes:
                        nodes[node_ip] = {
                            "tags": {},
                            "state": "terminated",
                        }
                assert len(nodes) == len(list_of_node_ips)
                with open(self.save_path, "w") as f:
                    logger.info(
                        "OnPremCoordinatorState: "
                        "Writing on prem coordinator state: {}".format(nodes)
                    )
                    f.write(json.dumps(nodes))


class LocalNodeProvider(NodeProvider):
    """NodeProvider for private/local clusters.

    `node_id` is overloaded to also be `node_ip` in this class.

    When `cluster_name` is provided, it manages a single cluster in a cluster
    specific state file. But when `cluster_name` is None, it manages multiple
    clusters in a unified state file that requires each node to be tagged with
    TAG_RAY_CLUSTER_NAME in create and non_terminated_nodes function calls to
    associate each node with the right cluster.

    The current use case of managing multiple clusters is by
    OnPremCoordinatorServer which receives node provider HTTP requests
    from CoordinatorSenderNodeProvider and uses LocalNodeProvider to get
    the responses.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        if cluster_name:
            lock_path = get_lock_path(cluster_name)
            state_path = get_state_path(cluster_name)
            self.state = ClusterState(
                lock_path,
                state_path,
                provider_config,
            )
            self.use_coordinator = False
        else:
            # LocalNodeProvider with a coordinator server.
            self.state = OnPremCoordinatorState(
                "/tmp/coordinator.lock",
                "/tmp/coordinator.state",
                provider_config["list_of_node_ips"],
            )
            self.use_coordinator = True

    def non_terminated_nodes(self, tag_filters):
        workers = self.state.get()
        matching_ips = []
        for worker_ip, info in workers.items():
            if info["state"] == "terminated":
                continue
            ok = True
            for k, v in tag_filters.items():
                if info["tags"].get(k) != v:
                    ok = False
                    break
            if ok:
                matching_ips.append(worker_ip)
        return matching_ips

    def is_running(self, node_id):
        return self.state.get()[node_id]["state"] == "running"

    def is_terminated(self, node_id):
        return not self.is_running(node_id)

    def node_tags(self, node_id):
        return self.state.get()[node_id]["tags"]

    def external_ip(self, node_id):
        """Returns an external ip if the user has supplied one.
        Otherwise, use the same logic as internal_ip below.

        This can be used to call ray up from outside the network, for example
        if the Ray cluster exists in an AWS VPC and we're interacting with
        the cluster from a laptop (where using an internal_ip will not work).

        Useful for debugging the local node provider with cloud VMs."""

        node_state = self.state.get()[node_id]
        ext_ip = node_state.get("external_ip")
        if ext_ip:
            return ext_ip
        else:
            return socket.gethostbyname(node_id)

    def internal_ip(self, node_id):
        return socket.gethostbyname(node_id)

    def set_node_tags(self, node_id, tags):
        with self.state.file_lock:
            info = self.state.get()[node_id]
            info["tags"].update(tags)
            self.state.put(node_id, info)

    def create_node(self, node_config, tags, count):
        """Creates min(count, currently available) nodes."""
        node_type = tags[TAG_RAY_NODE_KIND]
        with self.state.file_lock:
            workers = self.state.get()
            for node_id, info in workers.items():
                if info["state"] == "terminated" and (
                    self.use_coordinator or info["tags"][TAG_RAY_NODE_KIND] == node_type
                ):
                    info["tags"] = tags
                    info["state"] = "running"
                    self.state.put(node_id, info)
                    count = count - 1
                    if count == 0:
                        return

    def terminate_node(self, node_id):
        workers = self.state.get()
        info = workers[node_id]
        info["state"] = "terminated"
        self.state.put(node_id, info)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_local(cluster_config)


def record_local_head_state_if_needed(local_provider: LocalNodeProvider) -> None:
    """This function is called on the Ray head from StandardAutoscaler.reset
    to record the head node's own existence in the cluster state file.

    This is necessary because `provider.create_node` in
    `commands.get_or_create_head_node` records the head state on the
    cluster-launching machine but not on the head.
    """
    head_ip = local_provider.provider_config["head_ip"]
    cluster_name = local_provider.cluster_name
    # If the head node is not marked as created in the cluster state file,
    if head_ip not in local_provider.non_terminated_nodes({}):
        # These tags are based on the ones in commands.get_or_create_head_node;
        # keep in sync.
        head_tags = {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_USER_NODE_TYPE: LOCAL_CLUSTER_NODE_TYPE,
            TAG_RAY_NODE_NAME: "ray-{}-head".format(cluster_name),
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
        }
        # Mark the head node as created in the cluster state file.
        local_provider.create_node(node_config={}, tags=head_tags, count=1)

        assert head_ip in local_provider.non_terminated_nodes({})
