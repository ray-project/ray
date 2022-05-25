import logging

from collections import defaultdict
from typing import List, Optional, Dict
from pathlib import Path

from ray.experimental.log.common import (
    LogStreamOptions,
    FileIdentifiers
)
from ray.experimental.state.exception import DataSourceUnavailable
from ray.experimental.state.state_manager import StateDataSourceClient
# TODO(sang): Remove the usage of this class.
from ray.dashboard.datacenter import DataSource
from ray import ray_constants


logger = logging.getLogger(__name__)


class LogsManager:
    def __init__(self, data_source_client: StateDataSourceClient):
        self.client = data_source_client

    @property
    def logs_client(self) -> StateDataSourceClient:
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

    async def list_logs(self, node_id: str, glob_filter: Optional[str], timeout: int) -> List[str]:
        """Return a list of log files on a given node id filtered by the glob.
        
        Args:
            node_id: The node id where log files present.
            glob_filter: The glob filter to filter out log files.
            timeout: The timeout of the API.
        
        Returns:
            A list of log files categorized by a component.

        Raises:
            DataSourceUnavailable: If a source is unresponsive.
        """
        self._verify_node_registered(node_id)
        reply = await self.client.list_logs(node_id, timeout=timeout)
        return self._categorize_log_files(reply.log_files, glob_filter)

    async def create_log_stream(
        self,
        identifiers: FileIdentifiers,
        stream_options: LogStreamOptions,
    ):
        log_file_name, node_id = await self._resolve_file_and_node(identifiers)

        if stream_options.media_type == "stream":
            keep_alive = True
        elif stream_options.media_type == "file":
            keep_alive = False
        else:
            raise ValueError("Invalid media type: {media_type}")

        return await self.client.stream_log(
            node_id=node_id,
            log_file_name=log_file_name,
            keep_alive=keep_alive,
            lines=stream_options.lines,
            interval=stream_options.interval,
        )

    def _verify_node_registered(self, node_id: str):
        if node_id not in self.client.get_all_registered_agent_ids():
            raise DataSourceUnavailable(
                f"Given node id, {node_id} is not available. "
                "It's either the node is dead, or it is not registered. Use `ray list nodes` "
                "to see the node status. If the node is registered, it is highly likely "
                "a transient issue. Try again.")

    async def _resolve_file_and_node(self, file_identifier: FileIdentifiers, node_id: Optional[str] = None, node_ip: Optional[str] = None):
        node_id = node_id or self.resolve_node_id(node_ip)
        self._verify_node_registered(node_id)

        # If `log_file_name` is not provided, check if we can get the
        # corresponding `log_file_name` if an `actor_id` is provided.
        log_file_name = file_identifier.log_file_name
        if log_file_name is None:
            if file_identifier.actor_id is not None:
                actor_data = DataSource.actors.get(file_identifier.actor_id)
                if actor_data is None:
                    raise ValueError("Actor ID {actor_id} not found.")
                worker_id = actor_data["address"].get("workerId")
                if worker_id is None:
                    raise ValueError("Worker ID for Actor ID {actor_id} not found.")

                index = await self.list_logs(node_id, [worker_id])
                for node in index:
                    for file in index[node]["worker_stdout"]:
                        if file.split(".")[0].split("-")[1] == worker_id:
                            log_file_name = file
                            if node_id is None:
                                node_id = node
                            break

        # If `log_file_name` cannot be resolved, check if we can get the
        # corresponding `log_file_name` if a `pid` is provided.
        if log_file_name is None:
            pid = file_identifier.pid
            if pid is not None:
                if node_id is None:
                    raise ValueError(
                        "Node identifiers (node_ip, node_id) not provided "
                        f"with pid: {pid}. "
                    )
                index = await self.list_logs(node_id, [pid])
                for file in index[node_id]["worker_stdout"]:
                    if file.split(".")[0].split("-")[3] == pid:
                        log_file_name = file
                        break
                if log_file_name is None:
                    raise ValueError(
                        f"Worker with pid {pid} not found on node {node_id}"
                    )

        # node_id and log_file_name need to be known by this point
        if node_id is None or node_id not in self.client.get_all_registered_agent_ids():
            raise ValueError(f"node_id {node_id} not found")
        if log_file_name is None:
            raise ValueError("Could not resolve file identifiers to a file name.")

        return log_file_name, node_id

    def _categorize_log_files(log_files: List[str], glob_filter: Optional[str]) -> Dict[str, List[str]]:
        """Categorize the given log files after filterieng them out using a given glob.
        
        Returns:
            Dictionary of {component_name -> list of log files}
        """
        result = defaultdict(list)
        log_files = [p for p in log_files if Path(p).match(glob_filter)]
        for log_file in log_files:
            if "worker" in log_file and (log_file.endswith(".out") or log_file.endswith(".err")):
                result["worker"].append(log_file)
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
