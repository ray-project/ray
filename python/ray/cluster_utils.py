import copy
import logging
import json
import yaml
import os
import subprocess
import tempfile
import time

import ray
import ray._private.services
from ray._private.client_mode_hook import disable_client_hook
from ray import ray_constants

logger = logging.getLogger(__name__)


class AutoscalingCluster:
    """Create a local autoscaling cluster for testing.

    See test_autoscaler_fake_multinode.py for an end-to-end example.
    """

    def __init__(self, head_resources: dict, worker_node_types: dict):
        """Create the cluster.

        Args:
            head_resources: resources of the head node, including CPU.
            worker_node_types: autoscaler node types config for worker nodes.
        """
        self._head_resources = head_resources
        self._config = self._generate_config(head_resources, worker_node_types)
        self._process = None

    def _generate_config(self, head_resources, worker_node_types):
        base_config = yaml.safe_load(
            open(
                os.path.join(
                    os.path.dirname(ray.__file__),
                    "autoscaler/_private/fake_multi_node/example.yaml")))
        custom_config = copy.deepcopy(base_config)
        custom_config["available_node_types"] = worker_node_types
        custom_config["available_node_types"]["ray.head.default"] = {
            "resources": head_resources,
            "node_config": {},
            "max_workers": 0,
        }
        return custom_config

    def start(self):
        """Start the cluster.

        After this call returns, you can connect to the cluster with
        ray.init("auto").
        """
        subprocess.check_call(["ray", "stop", "--force"])
        fake_config = tempfile.mktemp()
        with open(fake_config, "w") as f:
            f.write(json.dumps(self._config))
        cmd = [
            "ray", "start", "--autoscaling-config={}".format(fake_config),
            "--head", "--block"
        ]
        if "CPU" in self._head_resources:
            cmd.append("--num-cpus={}".format(self._head_resources.pop("CPU")))
        if "GPU" in self._head_resources:
            cmd.append("--num-gpus={}".format(self._head_resources.pop("GPU")))
        if self._head_resources:
            cmd.append("--resources='{}'".format(
                json.dumps(self._head_resources)))
        env = os.environ.copy()
        env.update({
            "AUTOSCALER_UPDATE_INTERVAL_S": "1",
            "RAY_FAKE_CLUSTER": "1"
        })
        self._process = subprocess.Popen(cmd, env=env)
        time.sleep(5)  # TODO(ekl) wait for it properly

    def shutdown(self):
        """Terminate the cluster."""
        if self._process:
            self._process.kill()
        subprocess.check_call(["ray", "stop", "--force"])


class Cluster:
    def __init__(self,
                 initialize_head=False,
                 connect=False,
                 head_node_args=None,
                 shutdown_at_exit=True):
        """Initializes all services of a Ray cluster.

        Args:
            initialize_head (bool): Automatically start a Ray cluster
                by initializing the head node. Defaults to False.
            connect (bool): If `initialize_head=True` and `connect=True`,
                ray.init will be called with the redis address of this cluster
                passed in.
            head_node_args (dict): Arguments to be passed into
                `start_ray_head` via `self.add_node`.
            shutdown_at_exit (bool): If True, registers an exit hook
                for shutting down all started processes.
        """
        self.head_node = None
        self.worker_nodes = set()
        self.redis_address = None
        self.connected = False
        # Create a new global state accessor for fetching GCS table.
        self.global_state = ray.state.GlobalState()
        self._shutdown_at_exit = shutdown_at_exit
        if not initialize_head and connect:
            raise RuntimeError("Cannot connect to uninitialized cluster.")

        if initialize_head:
            head_node_args = head_node_args or {}
            self.add_node(**head_node_args)
            if connect:
                self.connect()

    @property
    def address(self):
        return self.redis_address

    def connect(self, namespace=None):
        """Connect the driver to the cluster."""
        assert self.redis_address is not None
        assert not self.connected
        output_info = ray.init(
            namespace=namespace,
            ignore_reinit_error=True,
            address=self.redis_address,
            _redis_password=self.redis_password)
        logger.info(output_info)
        self.connected = True

    def add_node(self, wait=True, **node_args):
        """Adds a node to the local Ray Cluster.

        All nodes are by default started with the following settings:
            cleanup=True,
            num_cpus=1,
            object_store_memory=150 * 1024 * 1024  # 150 MiB

        Args:
            wait (bool): Whether to wait until the node is alive.
            node_args: Keyword arguments used in `start_ray_head` and
                `start_ray_node`. Overrides defaults.

        Returns:
            Node object of the added Ray node.
        """
        default_kwargs = {
            "num_cpus": 1,
            "num_gpus": 0,
            "object_store_memory": 150 * 1024 * 1024,  # 150 MiB
            "min_worker_port": 0,
            "max_worker_port": 0,
            "dashboard_port": None,
        }
        ray_params = ray._private.parameter.RayParams(**node_args)
        ray_params.update_if_absent(**default_kwargs)
        with disable_client_hook():
            if self.head_node is None:
                node = ray.node.Node(
                    ray_params,
                    head=True,
                    shutdown_at_exit=self._shutdown_at_exit,
                    spawn_reaper=self._shutdown_at_exit)
                self.head_node = node
                self.redis_address = self.head_node.redis_address
                self.redis_password = node_args.get(
                    "redis_password", ray_constants.REDIS_DEFAULT_PASSWORD)
                self.webui_url = self.head_node.webui_url
                # Init global state accessor when creating head node.
                self.global_state._initialize_global_state(
                    self.redis_address, self.redis_password)
            else:
                ray_params.update_if_absent(redis_address=self.redis_address)
                # We only need one log monitor per physical node.
                ray_params.update_if_absent(include_log_monitor=False)
                # Let grpc pick a port.
                ray_params.update_if_absent(node_manager_port=0)
                node = ray.node.Node(
                    ray_params,
                    head=False,
                    shutdown_at_exit=self._shutdown_at_exit,
                    spawn_reaper=self._shutdown_at_exit)
                self.worker_nodes.add(node)

            if wait:
                # Wait for the node to appear in the client table. We do this
                # so that the nodes appears in the client table in the order
                # that the corresponding calls to add_node were made. We do
                # this because in the tests we assume that the driver is
                # connected to the first node that is added.
                self._wait_for_node(node)

        return node

    def remove_node(self, node, allow_graceful=True):
        """Kills all processes associated with worker node.

        Args:
            node (Node): Worker node of which all associated processes
                will be removed.
        """
        global_node = ray.worker._global_node
        if global_node is not None:
            if node._raylet_socket_name == global_node._raylet_socket_name:
                ray.shutdown()
                raise ValueError(
                    "Removing a node that is connected to this Ray client "
                    "is not allowed because it will break the driver."
                    "You can use the get_other_node utility to avoid removing"
                    "a node that the Ray client is connected.")

        if self.head_node == node:
            self.head_node.kill_all_processes(
                check_alive=False, allow_graceful=allow_graceful)
            self.head_node = None
            # TODO(rliaw): Do we need to kill all worker processes?
        else:
            node.kill_all_processes(
                check_alive=False, allow_graceful=allow_graceful)
            self.worker_nodes.remove(node)

        assert not node.any_processes_alive(), (
            "There are zombie processes left over after killing.")

    def _wait_for_node(self, node, timeout=30):
        """Wait until this node has appeared in the client table.

        Args:
            node (ray.node.Node): The node to wait for.
            timeout: The amount of time in seconds to wait before raising an
                exception.

        Raises:
            TimeoutError: An exception is raised if the timeout expires before
                the node appears in the client table.
        """
        ray._private.services.wait_for_node(self.redis_address,
                                            node.plasma_store_socket_name,
                                            self.redis_password, timeout)

    def wait_for_nodes(self, timeout=30):
        """Waits for correct number of nodes to be registered.

        This will wait until the number of live nodes in the client table
        exactly matches the number of "add_node" calls minus the number of
        "remove_node" calls that have been made on this cluster. This means
        that if a node dies without "remove_node" having been called, this will
        raise an exception.

        Args:
            timeout (float): The number of seconds to wait for nodes to join
                before failing.

        Raises:
            TimeoutError: An exception is raised if we time out while waiting
                for nodes to join.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            clients = self.global_state.node_table()
            live_clients = [client for client in clients if client["Alive"]]

            expected = len(self.list_all_nodes())
            if len(live_clients) == expected:
                logger.debug("All nodes registered as expected.")
                return
            else:
                logger.debug(
                    f"{len(live_clients)} nodes are currently registered, "
                    f"but we are expecting {expected}")
                time.sleep(0.1)
        raise TimeoutError("Timed out while waiting for nodes to join.")

    def list_all_nodes(self):
        """Lists all nodes.

        TODO(rliaw): What is the desired behavior if a head node
        dies before worker nodes die?

        Returns:
            List of all nodes, including the head node.
        """
        nodes = list(self.worker_nodes)
        if self.head_node:
            nodes = [self.head_node] + nodes
        return nodes

    def remaining_processes_alive(self):
        """Returns a bool indicating whether all processes are alive or not.

        Note that this ignores processes that have been explicitly killed,
        e.g., via a command like node.kill_raylet().

        Returns:
            True if all processes are alive and false otherwise.
        """
        return all(
            node.remaining_processes_alive() for node in self.list_all_nodes())

    def shutdown(self):
        """Removes all nodes."""

        # We create a list here as a copy because `remove_node`
        # modifies `self.worker_nodes`.
        all_nodes = list(self.worker_nodes)
        for node in all_nodes:
            self.remove_node(node)

        if self.head_node is not None:
            self.remove_node(self.head_node)
        # need to reset internal kv since gcs is down
        ray.experimental.internal_kv._internal_kv_reset()
