import copy
import json
import logging
import os
import subprocess
import tempfile
import time
from typing import Dict, Optional

import yaml

import ray
import ray._private.services
from ray._private import ray_constants
from ray._private.client_mode_hook import disable_client_hook
from ray._raylet import GcsClientOptions
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

cluster_not_supported = os.name == "nt"


@DeveloperAPI
class AutoscalingCluster:
    """Create a local autoscaling cluster for testing.

    See test_autoscaler_fake_multinode.py for an end-to-end example.
    """

    def __init__(
        self,
        head_resources: dict,
        worker_node_types: dict,
        autoscaler_v2: bool = False,
        **config_kwargs,
    ):
        """Create the cluster.

        Args:
            head_resources: resources of the head node, including CPU.
            worker_node_types: autoscaler node types config for worker nodes.
        """
        self._head_resources = head_resources
        self._config = self._generate_config(
            head_resources,
            worker_node_types,
            autoscaler_v2=autoscaler_v2,
            **config_kwargs,
        )
        self._autoscaler_v2 = autoscaler_v2

    def _generate_config(
        self, head_resources, worker_node_types, autoscaler_v2=False, **config_kwargs
    ):
        base_config = yaml.safe_load(
            open(
                os.path.join(
                    os.path.dirname(ray.__file__),
                    "autoscaler/_private/fake_multi_node/example.yaml",
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

        # Autoscaler v2 specific configs
        if autoscaler_v2:
            custom_config["provider"]["launch_multiple"] = True
            custom_config["provider"]["head_node_id"] = FAKE_HEAD_NODE_ID
        custom_config.update(config_kwargs)
        return custom_config

    def start(self, _system_config=None, override_env: Optional[Dict] = None):
        """Start the cluster.

        After this call returns, you can connect to the cluster with
        ray.init("auto").
        """
        subprocess.check_call(["ray", "stop", "--force"])
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
        if "object_store_memory" in self._head_resources:
            cmd.append(
                "--object-store-memory={}".format(
                    self._head_resources.pop("object_store_memory")
                )
            )
        if self._head_resources:
            cmd.append("--resources='{}'".format(json.dumps(self._head_resources)))
        if _system_config is not None:
            cmd.append(
                "--system-config={}".format(
                    json.dumps(_system_config, separators=(",", ":"))
                )
            )
        env = os.environ.copy()
        env.update({"AUTOSCALER_UPDATE_INTERVAL_S": "1", "RAY_FAKE_CLUSTER": "1"})
        if self._autoscaler_v2:
            # Set the necessary environment variables for autoscaler v2.
            env.update(
                {
                    "RAY_enable_autoscaler_v2": "1",
                    "RAY_CLOUD_INSTANCE_ID": FAKE_HEAD_NODE_ID,
                    "RAY_OVERRIDE_NODE_ID_FOR_TESTING": FAKE_HEAD_NODE_ID,
                }
            )
        if override_env:
            env.update(override_env)
        subprocess.check_call(cmd, env=env)

    def shutdown(self):
        """Terminate the cluster."""
        subprocess.check_call(["ray", "stop", "--force"])


@DeveloperAPI
class Cluster:
    def __init__(
        self,
        initialize_head: bool = False,
        connect: bool = False,
        head_node_args: dict = None,
        shutdown_at_exit: bool = True,
    ):
        """Initializes all services of a Ray cluster.

        Args:
            initialize_head: Automatically start a Ray cluster
                by initializing the head node. Defaults to False.
            connect: If `initialize_head=True` and `connect=True`,
                ray.init will be called with the address of this cluster
                passed in.
            head_node_args: Arguments to be passed into
                `start_ray_head` via `self.add_node`.
            shutdown_at_exit: If True, registers an exit hook
                for shutting down all started processes.
        """
        if cluster_not_supported:
            logger.warning(
                "Ray cluster mode is currently experimental and untested on "
                "Windows. If you are using it and running into issues please "
                "file a report at https://github.com/ray-project/ray/issues."
            )
        self.head_node = None
        self.worker_nodes = set()
        self.redis_address = None
        self.connected = False
        # Create a new global state accessor for fetching GCS table.
        self.global_state = ray._private.state.GlobalState()
        self._shutdown_at_exit = shutdown_at_exit
        if not initialize_head and connect:
            raise RuntimeError("Cannot connect to uninitialized cluster.")

        if initialize_head:
            head_node_args = head_node_args or {}
            self.add_node(**head_node_args)
            if connect:
                self.connect()

    @property
    def gcs_address(self):
        if self.head_node is None:
            return None
        return self.head_node.gcs_address

    @property
    def address(self):
        return self.gcs_address

    def connect(self, namespace=None):
        """Connect the driver to the cluster."""
        assert self.address is not None
        assert not self.connected
        output_info = ray.init(
            namespace=namespace,
            ignore_reinit_error=True,
            address=self.address,
            _redis_password=self.redis_password,
        )
        logger.info(output_info)
        self.connected = True

    def add_node(self, wait: bool = True, **node_args):
        """Adds a node to the local Ray Cluster.

        All nodes are by default started with the following settings:
            cleanup=True,
            num_cpus=1,
            object_store_memory=150 * 1024 * 1024  # 150 MiB

        Args:
            wait: Whether to wait until the node is alive.
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
                node = ray._private.node.Node(
                    ray_params,
                    head=True,
                    shutdown_at_exit=self._shutdown_at_exit,
                    spawn_reaper=self._shutdown_at_exit,
                )
                self.head_node = node
                self.redis_address = self.head_node.redis_address
                self.redis_password = node_args.get(
                    "redis_password", ray_constants.REDIS_DEFAULT_PASSWORD
                )
                self.webui_url = self.head_node.webui_url
                # Init global state accessor when creating head node.
                gcs_options = GcsClientOptions.create(
                    node.gcs_address,
                    None,
                    allow_cluster_id_nil=True,
                    fetch_cluster_id_if_nil=False,
                )
                self.global_state._initialize_global_state(gcs_options)
                # Write the Ray cluster address for convenience in unit
                # testing. ray.init() and ray.init(address="auto") will connect
                # to the local cluster.
                ray._private.utils.write_ray_address(self.head_node.gcs_address)
            else:
                ray_params.update_if_absent(redis_address=self.redis_address)
                ray_params.update_if_absent(gcs_address=self.gcs_address)
                # We only need one log monitor per physical node.
                ray_params.update_if_absent(include_log_monitor=False)
                # Let grpc pick a port.
                ray_params.update_if_absent(node_manager_port=0)

                node = ray._private.node.Node(
                    ray_params,
                    head=False,
                    shutdown_at_exit=self._shutdown_at_exit,
                    spawn_reaper=self._shutdown_at_exit,
                )
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
            node: Worker node of which all associated processes
                will be removed.
        """
        global_node = ray._private.worker._global_node
        if global_node is not None:
            if node._raylet_socket_name == global_node._raylet_socket_name:
                ray.shutdown()
                raise ValueError(
                    "Removing a node that is connected to this Ray client "
                    "is not allowed because it will break the driver."
                    "You can use the get_other_node utility to avoid removing"
                    "a node that the Ray client is connected."
                )

        node.destroy_external_storage()
        if self.head_node == node:
            # We have to wait to prevent the raylet becomes a zombie which will prevent
            # worker from exiting
            self.head_node.kill_all_processes(
                check_alive=False, allow_graceful=allow_graceful, wait=True
            )
            self.head_node = None
            # TODO(rliaw): Do we need to kill all worker processes?
        else:
            # We have to wait to prevent the raylet becomes a zombie which will prevent
            # worker from exiting
            node.kill_all_processes(
                check_alive=False, allow_graceful=allow_graceful, wait=True
            )
            self.worker_nodes.remove(node)

        assert (
            not node.any_processes_alive()
        ), "There are zombie processes left over after killing."

    def _wait_for_node(self, node, timeout: float = 30):
        """Wait until this node has appeared in the client table.

        Args:
            node (ray._private.node.Node): The node to wait for.
            timeout: The amount of time in seconds to wait before raising an
                exception.

        Raises:
            TimeoutError: An exception is raised if the timeout expires before
                the node appears in the client table.
        """
        ray._private.services.wait_for_node(
            node.gcs_address,
            node.plasma_store_socket_name,
            timeout,
        )

    def wait_for_nodes(self, timeout: float = 30):
        """Waits for correct number of nodes to be registered.

        This will wait until the number of live nodes in the client table
        exactly matches the number of "add_node" calls minus the number of
        "remove_node" calls that have been made on this cluster. This means
        that if a node dies without "remove_node" having been called, this will
        raise an exception.

        Args:
            timeout: The number of seconds to wait for nodes to join
                before failing.

        Raises:
            TimeoutError: An exception is raised if we time out while waiting
                for nodes to join.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            live_clients = self.global_state._live_node_ids()

            expected = len(self.list_all_nodes())
            if len(live_clients) == expected:
                logger.debug("All nodes registered as expected.")
                return
            else:
                logger.debug(
                    f"{len(live_clients)} nodes are currently registered, "
                    f"but we are expecting {expected}"
                )
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
        return all(node.remaining_processes_alive() for node in self.list_all_nodes())

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
        # Delete the cluster address.
        ray._private.utils.reset_ray_address()
