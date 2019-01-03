import binascii
import collections
import datetime
import errno
import logging
import multiprocessing
import os
import shutil
import sys
import tempfile
import time

import pyarrow.plasma as plasma

import ray.ray_constants
import ray.utils
from ray.utils import address, try_to_create_directory
from ray import runner
from ray.experimental import state

NIL_CLIENT_ID = ray.ray_constants.ID_SIZE * b"\xff"

PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_PLASMA_STORE = "plasma_store"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"

NODE_IP_ADDRESS = "node_ip_address"
REDIS_ADDRESS = "redis_address"
REDIS_SHARDS = "redis_shards"
OBJECT_STORE_ADDRESS = "object_store_address"
RAYLET_ADDRESS = "raylet_socket_name"
WEBUI_URL = "webui_url"

NOTEBOOK_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "WebUI.ipynb")

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)


class RayNodeSession(object):
    """A class that represents a Ray session in a node.

    Args:
        ray_params (ray.params.RayParams): The RayParams instance. The
            following parameters will be set to default values if it's None:
            node_ip_address("127.0.0.1"), include_webui(False),
            worker_path(path of default_worker.py),
            include_log_monitor(False)

    Attributes:
        all_processes (OrderedDict): This is a dictionary tracking all of the
            processes of different types that have been started by
            this services module. Note that the order of the keys is important
            because it determines the order in which these processes
            will be terminated when Ray exits, and certain orders will cause
            errors to be logged to the screen.
    """
    def __init__(self, ray_params, name=None):
        self.ray_params = ray_params

        if name is None:
            date_str = datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            name = "session_{date_str}_{pid}".format(
                pid=os.getpid(), date_str=date_str)
        self.name = name
        self.all_processes = collections.OrderedDict(
            [(PROCESS_TYPE_MONITOR, []), (PROCESS_TYPE_LOG_MONITOR, []),
             (PROCESS_TYPE_WORKER, []), (PROCESS_TYPE_RAYLET, []),
             (PROCESS_TYPE_PLASMA_STORE, []), (PROCESS_TYPE_REDIS_SERVER, []),
             (PROCESS_TYPE_WEB_UI, [])], )

        if ray_params.node_ip_address is None:
            # Get the node IP address if one is not provided.
            self.node_ip_address = ray.utils.get_node_ip_address()
        else:
            # Convert hostnames to numerical IP address.
            self.node_ip_address = ray.utils.address_to_ip(
                ray_params.node_ip_address)
        logger.info("Using IP address {} for this node."
                    .format(self.node_ip_address))

        if ray_params.redis_address is not None:
            self.redis_address = ray.utils.address_to_ip(
                ray_params.redis_address)
        else:
            self.redis_address = None

        self.redis_shards = None
        self.global_state = None
        self.plasma_store_socket_name = None
        self.raylet_socket_name = None
        self.webui_url = None
        self._temp_root = ray_params.temp_dir
        self.redirect_worker_output = ray_params.redirect_worker_output

        self._incremental_dict = collections.defaultdict(lambda: 0)

    @property
    def redis_port(self):
        if self.redis_address is None:
            return None
        else:
            return ray.utils.get_port(self.redis_address)

    def is_redis_server_started(self):
        return self.redis_address is not None

    def is_plasma_store_started(self):
        return self.plasma_store_socket_name is not None

    def is_raylet_started(self):
        return self.raylet_socket_name is not None

    @property
    def address_info(self):
        """
        A dictionary with address information for processes in a
        partially-started Ray cluster. If start_ray_local=True,
        any processes not in this dictionary will be started.
        If provided, an updated address_info dictionary will be returned to
        include processes that are newly started.
        """
        info = {
            NODE_IP_ADDRESS: self.node_ip_address,
            REDIS_ADDRESS: self.redis_address,
            REDIS_SHARDS: self.redis_shards,
            OBJECT_STORE_ADDRESS: self.plasma_store_socket_name,
            RAYLET_ADDRESS: self.raylet_socket_name,
            WEBUI_URL: self.webui_url,
        }
        return info

    def _record_process(self, proc_type, proc):
        self.all_processes[proc_type].append(proc)

    def _make_inc_temp(self, suffix="", prefix="", directory_name="/tmp/ray"):
        """Return a incremental temporary file name. The file is not created.

        Args:
            suffix (str): The suffix of the temp file.
            prefix (str): The prefix of the temp file.
            directory_name (str) : The base directory of the temp file.

        Returns:
            A string of file name. If there existing a file having the same
            name, the returned name will look like
            "{directory_name}/{prefix}.{unique_index}{suffix}"
        """
        directory_name = os.path.expanduser(directory_name)
        index = self._incremental_dict[suffix, prefix, directory_name]
        # `tempfile.TMP_MAX` could be extremely large,
        # so using `range` in Python2.x should be avoided.
        while index < tempfile.TMP_MAX:
            if index == 0:
                filename = os.path.join(directory_name, prefix + suffix)
            else:
                filename = os.path.join(directory_name,
                                        prefix + "." + str(index) + suffix)
            index += 1
            if not os.path.exists(filename):
                # Save the index.
                self._incremental_dict[suffix, prefix, directory_name] = index
                return filename

        raise FileExistsError(errno.EEXIST,
                              "No usable temporary filename found")

    def get_temp_root(self):
        """Get the path of the temporary root. If not existing,
        it will be created. """
        # Lazy creation. Avoid creating directories never used.
        if self._temp_root is None:
            self._temp_root = self._make_inc_temp(
                prefix=self.name, directory_name="/tmp/ray")
            try_to_create_directory(self._temp_root)
        return self._temp_root

    def _get_logs_dir_path(self):
        """Get a temp dir for logging."""
        logs_dir = os.path.join(self.get_temp_root(), "logs")
        try_to_create_directory(logs_dir)
        return logs_dir

    def _get_sockets_dir_path(self):
        """Get a temp dir for sockets."""
        sockets_dir = os.path.join(self.get_temp_root(), "sockets")
        try_to_create_directory(sockets_dir)
        return sockets_dir

    def _new_log_files(self, name, redirect_output):
        if not redirect_output:
            return None, None

        # Create a directory to be used for process log files.
        logs_dir = self._get_logs_dir_path()
        # Create another directory that will be used by some of the RL algorithms.

        # TODO(suquark): This is done by the old code.
        # We should be able to control its path later.
        try_to_create_directory("/tmp/ray")

        log_stdout = self._make_inc_temp(
            suffix=".out", prefix=name, directory_name=logs_dir)
        log_stderr = self._make_inc_temp(
            suffix=".err", prefix=name, directory_name=logs_dir)
        # Line-buffer the output (mode 1)
        log_stdout_file = open(log_stdout, "a", buffering=1)
        log_stderr_file = open(log_stderr, "a", buffering=1)
        return log_stdout_file, log_stderr_file

    def new_log_files(self, name, record_logs=True):
        """Generate partially randomized filenames for log files.

        Args:
            name (str): descriptive string for this log file.

        Returns:
            If redirect_output is true, this will return a tuple of two
                filehandles. The first is for redirecting stdout and the second
                is for redirecting stderr. If redirect_output is false,
                this will return a tuple of two None objects.
        """
        stdout_file, stderr_file = self._new_log_files(
            name, self.ray_params.redirect_output)
        if record_logs:
            self.global_state.record_log_files(self.node_ip_address,
                                               [stdout_file, stderr_file])
        return stdout_file, stderr_file

    def start_redis_server(self, cleanup=True):
        """Start the Redis global state store.

        Args:
            cleanup (bool): If cleanup is true, then the processes started here
                will be killed by services.cleanup() when the Python process
                that called this method exits.

        Returns:
            A tuple of the address for the primary Redis shard and a list of
                addresses for the remaining shards.
        """
        # We can not record logs here because the redis client has not been
        # initialized.
        redis_stdout_file, redis_stderr_file = self.new_log_files(
            "redis", record_logs=False)
        assigned_port, proc = runner.start_redis_instance(
            self.ray_params.redis_port,
            redis_max_clients=self.ray_params.redis_max_clients,
            password=self.ray_params.redis_password,
            # Below we use None to indicate no limit on the memory of the
            # primary Redis shard.
            redis_max_memory=None,
            use_credis=self.ray_params.use_credis,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file)

        if self.ray_params.redis_port is not None:
            assert assigned_port == self.ray_params.redis_port
        redis_address = address(self.node_ip_address, assigned_port)
        # This is when we initialize the Redis address
        self.redis_address = redis_address

        self.global_state = state.GlobalState(
            redis_address, redis_password=self.ray_params.redis_password)

        # Register the number of Redis shards in the primary shard,
        # so that clients know how many redis shards to expect
        # under RedisShards.
        self.global_state.num_redis_shards = self.ray_params.num_redis_shards
        # Put the redirect_worker_output bool in the Redis shard so that
        # workers can access it and know whether or not to redirect
        # their output.
        self.global_state.redirect_worker_output = (
            self.ray_params.redirect_worker_output)
        # Store version information in the primary Redis shard.
        self.global_state.version_info = ray.utils.get_version_info()

        self.global_state.record_log_files(self.node_ip_address,
                                           [redis_stdout_file,
                                            redis_stderr_file])
        if cleanup:
            self._record_process(PROCESS_TYPE_REDIS_SERVER, proc)

    def start_redis_shards(self, cleanup=True):
        """ Start other Redis shards. Each Redis shard logs to a separate file,
        prefixed by "redis-<shard number>"."""
        ray_params = self.ray_params

        # Cap the memory of the other redis shards if no limit is provided.
        redis_max_memory = (
            ray_params.redis_max_memory
            if ray_params.redis_max_memory is not None else
            ray.ray_constants.DEFAULT_REDIS_MAX_MEMORY_BYTES)

        if redis_max_memory < ray.ray_constants.REDIS_MINIMUM_MEMORY_BYTES:
            raise ValueError(
                "Attempting to cap Redis memory usage at {} bytes, "
                "but the minimum allowed is {} bytes.".format(
                    redis_max_memory,
                    ray.ray_constants.REDIS_MINIMUM_MEMORY_BYTES))

        redis_shards = []

        for i in range(ray_params.num_redis_shards):
            stdout_file, stderr_file = self.new_log_files(
                "redis-shard_{}".format(i))
            if ray_params.redis_shard_ports is None:
                port = None
            else:
                port = ray_params.redis_shard_ports[i]
            redis_shard_port, proc = runner.start_redis_instance(
                port,
                redis_max_clients=ray_params.redis_max_clients,
                password=ray_params.redis_password,
                redis_max_memory=redis_max_memory,
                use_credis=ray_params.use_credis,
                stdout_file=stdout_file,
                stderr_file=stderr_file)

            if port is not None:
                assert redis_shard_port == port

            shard_address = address(self.node_ip_address, redis_shard_port)

            if cleanup:
                self._record_process(PROCESS_TYPE_REDIS_SERVER, proc)
            redis_shards.append(shard_address)

        # Store redis shard information in the primary redis shard.
        self.global_state.append_redis_shards(redis_shards)

        if self.ray_params.use_credis:
            self.global_state.initialize_credis_shards(self.node_ip_address)

        self.redis_shards = redis_shards

    def connect_redis_server(self):
        ray_params = self.ray_params

        if self.redis_address is None:
            self.redis_address = ray_params.redis_address
        assert self.redis_address is not None

        redis_ip_address, redis_port = self.redis_address.split(":")
        # Wait for the Redis server to be started. And throw an exception if we
        # can't connect to it.
        runner.wait_for_redis_to_start(
            redis_ip_address, int(redis_port),
            password=ray_params.redis_password)

        self.global_state = state.GlobalState(
            self.redis_address,
            redis_password=ray_params.redis_password)

        self.global_state.check_version_info()
        self.global_state.check_no_existing_redis_clients(self.node_ip_address)
        # restore "redirect_worker_output"
        if self.redirect_worker_output is None:
            self.redirect_worker_output = (
                self.global_state.redirect_worker_output)

    def start_monitors(self, cleanup=True):
        # Start monitoring the processes.
        stdout_file, stderr_file = self.new_log_files("monitor")

        ray_params = self.ray_params

        monitor_proc = runner.start_monitor(
            self.redis_address,
            redis_password=ray_params.redis_password,
            autoscaling_config=ray_params.autoscaling_config,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

        if cleanup:
            self._record_process(PROCESS_TYPE_MONITOR, monitor_proc)

        raylet_monitor_proc = runner.start_raylet_monitor(
            self.redis_address,
            redis_password=ray_params.redis_password,
            config=ray_params.config,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

        if cleanup:
            self._record_process(PROCESS_TYPE_MONITOR, raylet_monitor_proc)

    def start_log_monitor(self, cleanup=True):
        """Start a log monitor process.

        Args:
            cleanup (bool): If cleanup is true, then the processes started here
                will be killed by services.cleanup() when the Python process
                that called this method exits.
        """
        ray_params = self.ray_params
        # Start monitoring the processes.
        stdout_file, stderr_file = self.new_log_files("log_monitor")
        p = runner.start_log_monitor(self.redis_address,
                                     self.node_ip_address,
                                     stdout_file,
                                     stderr_file,
                                     ray_params.redis_password)
        if cleanup:
            self._record_process(PROCESS_TYPE_LOG_MONITOR, p)

    def start_plasma_store(self, cleanup=True):
        """This method starts an object store process.

        Args:
            cleanup (bool): If cleanup is true, then the processes started here
                will be killed by services.cleanup() when the Python process
                that called this method exits.
        """

        ray_params = self.ray_params
        stdout_file, stderr_file = self.new_log_files("plasma_store")

        if ray_params.plasma_store_socket_name is None:
            socket_name = self._make_inc_temp(
                prefix="plasma_store",
                directory_name=self._get_sockets_dir_path())
        else:
            socket_name = ray_params.plasma_store_socket_name

        self.plasma_store_socket_name, p1 = runner.start_plasma_store(
            object_store_memory=ray_params.object_store_memory,
            plasma_directory=ray_params.plasma_directory,
            huge_pages=ray_params.huge_pages,
            plasma_store_socket_name=socket_name,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

        if cleanup:
            self._record_process(PROCESS_TYPE_PLASMA_STORE, p1)

    def _compute_num_workers(self):
        num_workers = self.ray_params.resources.get("CPU")
        if num_workers is None:
            num_workers = multiprocessing.cpu_count()
        return num_workers

    def _create_start_worker_command(self):
        """Create the command that the can be used to start workers."""
        ray_params = self.ray_params
        start_worker_command = ("{} {} "
                                "--node-ip-address={} "
                                "--object-store-name={} "
                                "--raylet-name={} "
                                "--redis-address={} "
                                "--temp-dir={}".format(
            sys.executable, ray_params.worker_path, self.node_ip_address,
            self.plasma_store_socket_name, self.raylet_socket_name,
            self.redis_address, self.get_temp_root()))

        if ray_params.redis_password:
            start_worker_command += " --redis-password {}".format(
                ray_params.redis_password)

        return start_worker_command

    def start_raylet(self, cleanup=True):
        """Start a raylet, which is a combined local scheduler
        and object manager.

        Args:
            cleanup (bool): If cleanup is true, then the processes started here
                will be killed by services.cleanup() when the Python process
                that called this method exits.
        """
        ray_params = self.ray_params
        raylet_stdout_file, raylet_stderr_file = self.new_log_files("raylet")

        # Worker needs the "raylet_socket_name".
        if ray_params.raylet_socket_name is not None:
            self.raylet_socket_name = ray_params.raylet_socket_name
        else:
            sockets_dir = self._get_sockets_dir_path()
            self.raylet_socket_name = self._make_inc_temp(
                prefix="raylet", directory_name=sockets_dir, suffix="")

        static_resources = ray.utils.check_and_update_resources(
            ray_params.resources)

        raylet_socket_name, pid = runner.start_raylet(
            self.node_ip_address, self.redis_address,
            self.plasma_store_socket_name,
            self.raylet_socket_name,
            temp_dir=self.get_temp_root(),
            static_resources=static_resources,
            start_worker_command=self._create_start_worker_command(),
            num_workers=self._compute_num_workers(),
            # If the object manager port is None, then use 0 to cause the
            # object manager to choose its own port.
            object_manager_port=ray_params.object_manager_port or 0,
            # If the node manager port is None, then use 0 to cause the
            # node manager to choose its own port.
            node_manager_port=ray_params.node_manager_port or 0,
            redis_password=ray_params.redis_password,
            stdout_file=raylet_stdout_file,
            stderr_file=raylet_stderr_file,
            config=ray_params.config)

        assert raylet_socket_name == self.raylet_socket_name

        if cleanup:
            self._record_process(PROCESS_TYPE_RAYLET, pid)

    def start_ui(self, cleanup=True):
        """Start a UI process.

        Args:
            cleanup (bool): If cleanup is true, then the processes started here
                will be killed by services.cleanup() when the Python process
                that called this method exits.
        """
        ui_stdout_file, ui_stderr_file = self.new_log_files("webui")
        port = ray.utils.get_usable_port(8888)
        # We generate the token used for authentication ourselves to avoid
        # querying the jupyter server.
        notebook_name = self._make_inc_temp(
            suffix=".ipynb", prefix="ray_ui",
            directory_name=self.get_temp_root())
        # We copy the notebook file so that the original doesn't get modified
        # by the user.
        shutil.copy(NOTEBOOK_FILE_PATH, notebook_name)
        new_notebook_directory = os.path.dirname(notebook_name)
        token = ray.utils.decode(binascii.hexlify(os.urandom(24)))
        webui_url = ("http://localhost:{}/notebooks/{}?token={}".format(
            port, os.path.basename(notebook_name), token))

        webui_url, ui_process = runner.start_ui(port, new_notebook_directory,
                                                webui_url, token,
                                                self.redis_address,
                                                ui_stdout_file, ui_stderr_file)
        if ui_process is not None and cleanup:
            self._record_process(PROCESS_TYPE_WEB_UI, ui_process)
        self.webui_url = webui_url

    def start_ray_processes(self, cleanup=True):
        """Helper method to start Ray processes.

        Args:
            cleanup (bool): If cleanup is true, then the processes started here
                will be killed by services.cleanup() when the Python process
                that called this method exits.
        Returns:
            A dictionary of the address information for the processes that were
                started.
        """
        ray_params = self.ray_params

        logger.info(
            "Process STDOUT and STDERR is being redirected to {}.".format(
                self._get_logs_dir_path()))

        ray_params.update_if_absent(
            resources={},
            include_log_monitor=False,
            include_webui=False,
            worker_path=runner.DEFAULT_WORKER_PATH)

        # Start Redis if there isn't already an instance running.
        # TODO(rkn): We are suppressing the output of Redis because on Linux
        # it prints a bunch of warning messages when it starts up.
        # Instead of suppressing the output, we should address the warnings.
        if not self.is_redis_server_started():
            self.start_redis_server(cleanup=cleanup)
            self.start_redis_shards(cleanup=cleanup)
            time.sleep(0.1)
            # Start monitoring the processes.
            self.start_monitors(cleanup=cleanup)
        else:
            self.connect_redis_server()

        self.global_state.initialize_global_state()

        if self.redis_shards == []:
            # Get redis shards from primary redis instance.
            self.redis_shards = self.global_state.get_redis_shards()

        # Start the log monitor, if necessary.
        if ray_params.include_log_monitor:
            self.start_log_monitor(cleanup=cleanup)

        # Start any object stores that do not yet exist.
        if not self.is_plasma_store_started():
            # Start plasma object store.
            self.start_plasma_store(cleanup=cleanup)
            time.sleep(0.1)

        # Start any raylets that do not exist yet.
        if not self.is_raylet_started():
            self.start_raylet(cleanup=cleanup)

        # Try to start the web UI.
        if ray_params.include_webui:
            self.start_ui(cleanup=cleanup)
        else:
            self.webui_url = ""

    def connect_cluster(self, session_index=0):
        ray_params = self.ray_params
        assert self.redis_address is not None
        self.global_state = state.GlobalState(
            self.redis_address,
            redis_password=ray_params.redis_password)
        self.global_state.initialize_global_state()

        if (ray_params.raylet_socket_name is not None
            and ray_params.plasma_store_socket_name is not None):
            self.plasma_store_socket_name = ray_params.plasma_store_socket_name
            self.raylet_socket_name = ray_params.raylet_socket_name
        else:
            raylets = self.global_state.get_client_info(
                self.node_ip_address, self.redis_address)
            # Make sure that at least one raylet has started locally.
            # This handles a race condition where Redis has started but
            # the raylet has not connected.
            if len(raylets) == 0:
                raise Exception(
                    "Redis has started but no raylets have registered yet.")
            client_table_info = raylets[session_index]
            self.plasma_store_socket_name = ray.utils.decode(
                client_table_info.ObjectStoreSocketName())
            self.raylet_socket_name = ray.utils.decode(
                client_table_info.RayletSocketName())

        # Web UI should be running.
        self.webui_url = self.global_state.webui

    def all_processes_alive(self, exclude=None):
        """Check if all of the processes are still alive.

        Args:
            exclude: Don't check the processes whose types are in this list.
        """

        if exclude is None:
            exclude = []
        for process_type, processes in self.all_processes.items():
            # Note that p.poll() returns the exit code that the process exited
            # with, so an exit code of None indicates that the process is still
            # alive.
            processes_alive = [p.poll() is None for p in processes]
            if not all(processes_alive) and process_type not in exclude:
                logger.warning(
                    "A process of type {} has died.".format(process_type))
                return False
        return True

    def shutdown(self):
        """When running in local mode, shutdown the Ray processes.

        This method is used to shutdown processes that were started with
        services.start_ray_head(). It kills all scheduler, object store,
        and worker processes that were started by this services module.
        Driver processes are started and disconnected by worker.py.
        """
        successfully_shut_down = True
        # Terminate the processes in reverse order.
        for process_type in self.all_processes.keys():
            # Kill all of the processes of a certain type.
            for p in self.all_processes[process_type]:
                success = runner.kill_process(p)
                successfully_shut_down = successfully_shut_down and success
            # Reset the list of processes of this type.
            self.all_processes[process_type] = []
        if not successfully_shut_down:
            logger.warning("Ray did not shut down properly.")

    def register_driver(self, worker_id, name):
        """Register the driver/job with Redis.

        The concept of a driver is the same as the concept of a "job".

        Args:
            worker_id (ObjectID): The ID of the driver.
            name: The name of the driver.
        """
        self.global_state.register_driver(
            worker_id, name, node_ip_address=self.node_ip_address,
            plasma_store_socket_name=self.plasma_store_socket_name,
            raylet_socket_name=self.raylet_socket_name,
            webui_url=self.webui_url)

    def setup_worker(self, worker_id):
        # Register the worker with Redis.
        if self.redirect_worker_output:
            stdout_file, stderr_file = self._new_log_files(
                "worker-" + ray.utils.binary_to_hex(worker_id), True)
            self.global_state.record_log_files(self.node_ip_address,
                                               [stdout_file, stderr_file])
            sys.stdout = stdout_file
            sys.stderr = stderr_file
        else:
            stdout_file, stderr_file = None, None
        self.global_state.register_worker(
            worker_id,
            node_ip_address=self.node_ip_address,
            plasma_store_socket_name=self.plasma_store_socket_name,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

    def create_plasma_client(self):
        return plasma.connect(self.plasma_store_socket_name)

    def create_redis_client(self):
        assert self.redis_address is not None
        return runner.create_redis_client(self.redis_address,
                                          self.ray_params.redis_password)
