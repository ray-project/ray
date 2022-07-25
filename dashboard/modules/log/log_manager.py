import logging
import re

from collections import defaultdict
from typing import List, Optional, Dict, AsyncIterable, Tuple, Callable

from ray.experimental.state.common import GetLogOptions
from ray.experimental.state.exception import DataSourceUnavailable
from ray.experimental.state.state_manager import StateDataSourceClient

# TODO(sang): Remove the usage of this class.
from ray.dashboard.datacenter import DataSource


logger = logging.getLogger(__name__)

WORKER_LOG_PATTERN = re.compile(".*worker-([0-9a-f]+)-([0-9a-f]+)-(\d+).out")


class LogsManager:
    def __init__(self, data_source_client: StateDataSourceClient):
        self.client = data_source_client

    @property
    def data_source_client(self) -> StateDataSourceClient:
        return self.client

    def ip_to_node_id(self, node_ip: Optional[str]):
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
        node_id = options.node_id or self.ip_to_node_id(options.node_ip)

        log_file_name, node_id = await self.resolve_filename(
            node_id=node_id,
            log_filename=options.filename,
            actor_id=options.actor_id,
            task_id=options.task_id,
            pid=options.pid,
            get_actor_fn=DataSource.actors.get,
            timeout=options.timeout,
        )

        keep_alive = options.media_type == "stream"
        stream = await self.client.stream_log(
            node_id=node_id,
            log_file_name=log_file_name,
            keep_alive=keep_alive,
            lines=options.lines,
            interval=options.interval,
            # If we keepalive logs connection, we shouldn't have timeout
            # otherwise the stream will be terminated forcefully
            # after the deadline is expired.
            timeout=options.timeout if not keep_alive else None,
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

    async def resolve_filename(
        self,
        *,
        node_id: str,
        log_filename: Optional[str],
        actor_id: Optional[str],
        task_id: Optional[str],
        pid: Optional[str],
        get_actor_fn: Callable[[str], Dict],
        timeout: int,
    ) -> Tuple[str, str]:
        """Return the file name given all options."""
        if actor_id:
            actor_data = get_actor_fn(actor_id)
            if actor_data is None:
                raise ValueError(f"Actor ID {actor_id} not found.")

            # TODO(sang): Only the latest worker id can be obtained from
            # actor information now. That means, if actors are restarted,
            # there's no way for us to get the past worker ids.
            worker_id = actor_data["address"].get("workerId")
            if not worker_id:
                raise ValueError(
                    f"Worker ID for Actor ID {actor_id} not found. "
                    "Actor is not scheduled yet."
                )
            node_id = actor_data["address"].get("rayletId")
            if not node_id:
                raise ValueError(
                    f"Node ID for Actor ID {actor_id} not found. "
                    "Actor is not scheduled yet."
                )
            self._verify_node_registered(node_id)

            # List all worker logs that match actor's worker id.
            log_files = await self.list_logs(
                node_id, timeout, glob_filter=f"*{worker_id}*"
            )

            # Find matching worker logs.
            for filename in log_files["worker_out"]:
                # Worker logs look like worker-[worker_id]-[job_id]-[pid].log
                worker_id_from_filename = WORKER_LOG_PATTERN.match(filename).group(1)
                if worker_id_from_filename == worker_id:
                    log_filename = filename
                    break
        elif task_id:
            raise NotImplementedError("task_id is not supported yet.")
        elif pid:
            self._verify_node_registered(node_id)
            log_files = await self.list_logs(node_id, timeout, glob_filter=f"*{pid}*")
            for filename in log_files["worker_out"]:
                # worker-[worker_id]-[job_id]-[pid].log
                worker_pid_from_filename = int(
                    WORKER_LOG_PATTERN.match(filename).group(3)
                )
                if worker_pid_from_filename == pid:
                    log_filename = filename
                    break

        if log_filename is None:
            raise FileNotFoundError(
                "Could not find a log file. Please make sure the given "
                "option exists in the cluster.\n"
                f"\node_id: {node_id}\n"
                f"\filename: {log_filename}\n"
                f"\tactor_id: {actor_id}\n"
                f"\task_id: {task_id}\n"
                f"\tpid: {pid}\n"
            )

        return log_filename, node_id

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
