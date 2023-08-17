"""Command runners specific to TPU VM pods.

TPU VM pods may contain multiple hosts, each including attached TPU chips and
associated internal/external IP addresses.

To support TPU VM pods, we represent entire TPU pods as "Ray Nodes", meaning
that TPU pods will need to run the operations specified in `CommandRunnerInterface`
N times, where N denotes the number of hosts that comprise a TPU pod.

To maintain feature completeness, we simply wrap the existing `SSHCommandRunner` and
`DockerCommandRunner` and run them as batched calls.

"""
from concurrent.futures import ThreadPoolExecutor
from types import ModuleType
from typing import Any, Dict, Optional

from ray._private import ray_constants
from ray.autoscaler._private.command_runner import DockerCommandRunner, SSHCommandRunner
from ray.autoscaler._private.gcp.node import GCPTPUNode
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler.node_provider import NodeProvider


class TPUVMSSHCommandRunner(SSHCommandRunner):
    """An SSH command runner with overwritten IP address calls."""

    def __init__(self, internal_ip: str, external_ip: str, *args, **kwargs):
        self._internal_ip = internal_ip
        self._external_ip = external_ip
        super().__init__(*args, **kwargs)

    def _get_node_ip(self) -> str:
        if self.use_internal_ip:
            return self._internal_ip
        else:
            return self._external_ip


class TPUVMDockerCommandRunner(DockerCommandRunner):
    """A Docker command runner with overwritten IP addresses."""

    def __init__(
        self,
        docker_config: Dict[str, Any],
        internal_ip: str,
        external_ip: str,
        **common_args
    ):
        super().__init__(docker_config=docker_config, **common_args)
        self.ssh_command_runner = TPUVMSSHCommandRunner(
            internal_ip=internal_ip, external_ip=external_ip, **common_args
        )


class TPUCommandRunner(CommandRunnerInterface):
    """A TPU pod command runner."""

    def __init__(
        self,
        instance: GCPTPUNode,
        log_prefix: str,
        node_id: str,
        auth_config: Dict[str, Any],
        provider: NodeProvider,
        cluster_name: str,
        process_runner: ModuleType,
        use_internal_ip: bool,
        docker_config: Optional[Dict[str, Any]] = None,
    ):
        def create_command_runner(
            worker_id: int, internal_ip: str, external_ip: str
        ) -> CommandRunnerInterface:
            """Returns the correct base command runner."""

            common_args = {
                "internal_ip": internal_ip,
                "external_ip": external_ip,
                "log_prefix": "[tpu_worker_{}] ".format(worker_id) + log_prefix,
                "node_id": node_id,
                "provider": provider,
                "auth_config": auth_config,
                "cluster_name": cluster_name,
                "process_runner": process_runner,
                "use_internal_ip": use_internal_ip,
            }
            if docker_config and docker_config["container_name"] != "":
                return TPUVMDockerCommandRunner(
                    docker_config=docker_config, **common_args
                )
            else:
                return TPUVMSSHCommandRunner(**common_args)

        self._command_runners = []
        self._num_workers = instance.num_workers
        for i in range(self._num_workers):
            self._command_runners.append(
                create_command_runner(
                    worker_id=i,
                    internal_ip=instance.get_internal_ip(i),
                    external_ip=instance.get_external_ip(i),
                )
            )

    @property
    def num_connections(self) -> int:
        """Return the number of active connections allowed at a time.

        We occasionally see issues where too many concurrent connections may lead to
        failed SSH connections when there are too many TPU hosts.

        We utilize this property to cap the maximum number of active connections
        at a time until a proper fix is found.

        """
        num_max_concurrent_active_connections = ray_constants.env_integer(
            ray_constants.RAY_TPU_MAX_CONCURRENT_CONNECTIONS_ENV_VAR, default=16
        )
        return min(self._num_workers, num_max_concurrent_active_connections)

    def run(self, *args, **kwargs) -> str:
        with ThreadPoolExecutor(self.num_connections) as executor:
            results = executor.map(
                lambda i: self._command_runners[i].run(*args, **kwargs),
                range(self._num_workers),
            )
        # Note: the `run` abstract function may return a string representing
        # representing command output, but this result is rarely used - especially
        # if the node is a worker (which a TPU pod is).
        # We return only the results from worker 0 which may not always be expected.
        return list(results)[0]

    def run_rsync_up(self, *args, **kwargs) -> None:
        with ThreadPoolExecutor(self.num_connections) as executor:
            executor.map(
                lambda i: self._command_runners[i].run_rsync_up(*args, **kwargs),
                range(self._num_workers),
            )

    def run_rsync_down(self, *args, **kwargs) -> None:
        """Rsync files down from the cluster node.

        Args:
            source: The (remote) source directory or file.
            target: The (local) destination path.
        """
        with ThreadPoolExecutor(self.num_connections) as executor:
            executor.map(
                lambda i: self._command_runners[i].run_rsync_down(*args, **kwargs),
                range(self._num_workers),
            )

    def remote_shell_command_str(self) -> str:
        """Return the command the user can use to open a shell."""
        # Note: this function is rarely used if the node is a worker.
        # We return only the results from worker 0 which may not always be expected.
        return self._command_runners[0].remote_shell_command_str()

    def run_init(self, *args, **kwargs) -> Optional[bool]:
        """Used to run extra initialization commands.

        Args:
            as_head: Run as head image or worker.
            file_mounts: Files to copy to the head and worker nodes.
            sync_run_yet: Whether sync has been run yet.

        Returns:
            optional: Whether initialization is necessary.
        """
        with ThreadPoolExecutor(self.num_connections) as executor:
            results = executor.map(
                lambda i: self._command_runners[i].run_init(*args, **kwargs),
                range(self._num_workers),
            )
        # Note: the `run_init` abstract function may return a bool representing
        # whether initialization is necessary, but this result is rarely used -
        # especially if the node is a worker (which a TPU pod is).
        # Here we return whether any workers require initialization, which may not be
        # the expected result.
        return any(results)
