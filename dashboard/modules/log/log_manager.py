import logging

from collections import defaultdict
from typing import List, Optional, Dict, AsyncIterable, Tuple, Callable

from ray._private.log_monitor import JOB_LOG_PATTERN
from ray.core.generated.gcs_pb2 import ActorTableData
from ray.experimental.state.common import GetLogOptions
from ray.experimental.state.exception import DataSourceUnavailable
from ray.experimental.state.state_manager import StateDataSourceClient

# TODO(sang): Remove the usage of this class.
from ray.dashboard.datacenter import DataSource


logger = logging.getLogger(__name__)


class LogsManager:
    def __init__(self, data_source_client: StateDataSourceClient):
        self.client = data_source_client

    @property
    def data_source_client(self) -> StateDataSourceClient:
        return self.client

    def resolve_node_id(self, node_ip: Optional[str]):
        """Resolve the node id from a given node ip.

        Args:
            node_ip: The node ip.

        Returns:
            node_id if there's a node id that matches the given node ip and is alive.
            None otherwise.
        """
        return self.client.ip_to_node_id(node_ip)

    async def list_logs(
        self, node_id: str, timeout: int, glob_filter: str = "*"
    ) -> Dict[str, List[str]]:
        """Return a list of log files on a given node id filtered by the glob.

        Args:
            node_id: The node id where log files present.
            timeout: The timeout of the API.
            glob_filter: The glob filter to filter out log files.

        Returns:
            Dictionary of {component_name -> list of log files}

        Raises:
            DataSourceUnavailable: If a source is unresponsive.
        """
        self._verify_node_registered(node_id)
        reply = await self.client.list_logs(node_id, glob_filter, timeout=timeout)
        return self._categorize_log_files(reply.log_files)

    async def stream_logs(
        self,
        options: GetLogOptions,
    ) -> AsyncIterable[bytes]:
        """Generate a stream of logs in bytes.

        Args:
            options: The option for streaming logs.

        Return:
            Async generator of streamed logs in bytes.
        """
        log_file_name, node_id = await self._resolve_file_and_node(
            options, DataSource.actors.get
        )

        keep_alive = options.media_type == "stream"
        stream = await self.client.stream_log(
            node_id=node_id,
            log_file_name=log_file_name,
            keep_alive=keep_alive,
            lines=options.lines,
            interval=options.interval,
            timeout=options.timeout,
        )

        async for streamed_log in stream:
            yield streamed_log.data

    def _verify_node_registered(self, node_id: str):
        if node_id not in self.client.get_all_registered_agent_ids():
            raise DataSourceUnavailable(
                f"Given node id {node_id} is not available. "
                "It's either the node is dead, or it is not registered. "
                "Use `ray list nodes` "
                "to see the node status. If the node is registered, "
                "it is highly likely "
                "a transient issue. Try again."
            )
        assert node_id is not None

    async def _resolve_file_and_node(
        self, options: GetLogOptions, get_actor_fn: Callable[[str], ActorTableData]
    ) -> Tuple[str, str]:
        """Return the file name and a node id of that file based on the given option."""
        node_id = options.node_id or self.resolve_node_id(options.node_ip)
        self._verify_node_registered(node_id)

        # If `log_file_name` is not provided, check if we can get the
        # corresponding `log_file_name` if an `actor_id` is provided.
        log_file_name = options.filename
        if log_file_name is None:
            if options.actor_id is not None:
                actor_data = get_actor_fn(options.actor_id)
                if actor_data is None:
                    raise ValueError(f"Actor ID {options.actor_id} not found.")
                worker_id = actor_data["address"].get("workerId")
                if worker_id is None:
                    raise ValueError(
                        f"Worker ID for Actor ID {options.actor_id} not found. "
                        "Actor is not scheduled yet."
                    )

                log_files = await self.list_logs(
                    node_id, options.timeout, glob_filter=f"*{worker_id}*"
                )
                for file in log_files["worker_out"]:
                    if file.split(".")[0].split("-")[1] == worker_id:
                        log_file_name = file
                        break

        # If `log_file_name` cannot be resolved, check if we can get the
        # corresponding `log_file_name` if a `pid` is provided.
        if log_file_name is None:
            pid = options.pid
            if pid is not None:
                log_files = await self.list_logs(
                    node_id, timeout=options.timeout, glob_filter=f"*{pid}*"
                )
                logger.info(log_files)
                for file in log_files["worker_out"]:
                    worker_pid = int(JOB_LOG_PATTERN.match(file).group(2))
                    if worker_pid == pid:
                        log_file_name = file
                        break
                if log_file_name is None:
                    raise ValueError(
                        f"Worker with pid {pid} not found on node {node_id}"
                    )

        if log_file_name is None:
            raise ValueError(f"Could not find a log file for a given option {options}")

        return log_file_name, node_id

    def _categorize_log_files(self, log_files: List[str]) -> Dict[str, List[str]]:
        """Categorize the given log files after filterieng them out using a given glob.

        Returns:
            Dictionary of {component_name -> list of log files}
        """
        result = defaultdict(list)
        for log_file in log_files:
            if "worker" in log_file and (log_file.endswith(".out")):
                result["worker_out"].append(log_file)
            elif "worker" in log_file and (log_file.endswith(".err")):
                result["worker_err"].append(log_file)
            elif "core-worker" in log_file and log_file.endswith(".log"):
                result["core_worker"].append(log_file)
            elif "core-driver" in log_file and log_file.endswith(".log"):
                result["driver"].append(log_file)
            elif "raylet." in log_file:
                result["raylet"].append(log_file)
            elif "gcs_server." in log_file:
                result["gcs_server"].append(log_file)
            elif "log_monitor" in log_file:
                result["internal"].append(log_file)
            elif "monitor" in log_file:
                result["autoscaler"].append(log_file)
            elif "agent." in log_file:
                result["agent"].append(log_file)
            elif "dashboard." in log_file:
                result["dashboard"].append(log_file)
            else:
                result["internal"].append(log_file)

        return result
