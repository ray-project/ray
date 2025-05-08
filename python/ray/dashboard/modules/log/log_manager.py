import logging
import re
from collections import defaultdict
from typing import AsyncIterable, Awaitable, Callable, Dict, List, Optional, Tuple

from ray import ActorID, NodeID, WorkerID
from ray._private.pydantic_compat import BaseModel
from ray.core.generated.gcs_pb2 import ActorTableData
from ray.dashboard.modules.job.common import JOB_LOGS_PATH_TEMPLATE
from ray.util.state.common import (
    DEFAULT_RPC_TIMEOUT,
    GetLogOptions,
    protobuf_to_task_state_dict,
)
from ray.util.state.state_manager import StateDataSourceClient

if BaseModel is None:
    raise ModuleNotFoundError("Please install pydantic via `pip install pydantic`.")


logger = logging.getLogger(__name__)

WORKER_LOG_PATTERN = re.compile(r".*worker-([0-9a-f]+)-([0-9a-f]+)-(\d+).(out|err)")


class ResolvedStreamFileInfo(BaseModel):
    # The node id where the log file is located.
    node_id: str

    # The log file path name. Could be a relative path relative to ray's logging folder,
    # or an absolute path.
    filename: str

    # Start offset in the log file to stream from. None to indicate beginning of
    # the file, or determined by last tail lines.
    start_offset: Optional[int]

    # End offset in the log file to stream from. None to indicate the end of the file.
    end_offset: Optional[int]


class LogsManager:
    def __init__(self, data_source_client: StateDataSourceClient):
        self.client = data_source_client

    @property
    def data_source_client(self) -> StateDataSourceClient:
        return self.client

    async def ip_to_node_id(self, node_ip: Optional[str]) -> Optional[str]:
        """Resolve the node id in hex from a given node ip.

        Args:
            node_ip: The node ip.

        Returns:
            node_id if there's a node id that matches the given node ip and is alive.
            None otherwise.
        """
        return await self.client.ip_to_node_id(node_ip)

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
            ValueError: If a source is unresponsive.
        """
        reply = await self.client.list_logs(node_id, glob_filter, timeout=timeout)
        return self._categorize_log_files(reply.log_files)

    async def stream_logs(
        self,
        options: GetLogOptions,
        get_actor_fn: Callable[[ActorID], Awaitable[Optional[ActorTableData]]],
    ) -> AsyncIterable[bytes]:
        """Generate a stream of logs in bytes.

        Args:
            options: The option for streaming logs.

        Return:
            Async generator of streamed logs in bytes.
        """
        node_id = options.node_id
        if node_id is None:
            node_id = await self.ip_to_node_id(options.node_ip)

        res = await self.resolve_filename(
            node_id=node_id,
            log_filename=options.filename,
            actor_id=options.actor_id,
            task_id=options.task_id,
            attempt_number=options.attempt_number,
            pid=options.pid,
            get_actor_fn=get_actor_fn,
            timeout=options.timeout,
            suffix=options.suffix,
            submission_id=options.submission_id,
        )

        keep_alive = options.media_type == "stream"
        stream = await self.client.stream_log(
            node_id=res.node_id,
            log_file_name=res.filename,
            keep_alive=keep_alive,
            lines=options.lines,
            interval=options.interval,
            # If we keepalive logs connection, we shouldn't have timeout
            # otherwise the stream will be terminated forcefully
            # after the deadline is expired.
            timeout=options.timeout if not keep_alive else None,
            start_offset=res.start_offset,
            end_offset=res.end_offset,
        )

        async for streamed_log in stream:
            yield streamed_log.data

    async def _resolve_job_filename(self, sub_job_id: str) -> Tuple[str, str]:
        """Return the log file name and node id for a given job submission id.

        Args:
            sub_job_id: The job submission id.

        Returns:
            The log file name and node id.
        """
        job_infos = await self.client.get_job_info(timeout=DEFAULT_RPC_TIMEOUT)
        target_job = None
        for job_info in job_infos:
            if job_info.submission_id == sub_job_id:
                target_job = job_info
                break
        if target_job is None:
            logger.info(f"Submission job ID {sub_job_id} not found.")
            return None, None

        node_id = job_info.driver_node_id
        if node_id is None:
            raise ValueError(
                f"Job {sub_job_id} has no driver node id info. "
                "This is likely a bug. Please file an issue."
            )

        log_filename = JOB_LOGS_PATH_TEMPLATE.format(submission_id=sub_job_id)
        return node_id, log_filename

    async def _resolve_worker_file(
        self,
        node_id_hex: str,
        worker_id_hex: Optional[str],
        pid: Optional[int],
        suffix: str,
        timeout: int,
    ) -> Optional[str]:
        """Resolve worker log file."""
        if worker_id_hex is not None and pid is not None:
            raise ValueError(
                f"Only one of worker id({worker_id_hex}) or pid({pid}) should be"
                "provided."
            )

        if worker_id_hex is not None:
            log_files = await self.list_logs(
                node_id_hex, timeout, glob_filter=f"*{worker_id_hex}*{suffix}"
            )
        else:
            log_files = await self.list_logs(
                node_id_hex, timeout, glob_filter=f"*{pid}*{suffix}"
            )

        # Find matching worker logs.
        for filename in [*log_files["worker_out"], *log_files["worker_err"]]:
            # Worker logs look like worker-[worker_id]-[job_id]-[pid].out
            if worker_id_hex is not None:
                worker_id_from_filename = WORKER_LOG_PATTERN.match(filename).group(1)
                if worker_id_from_filename == worker_id_hex:
                    return filename
            else:
                worker_pid_from_filename = int(
                    WORKER_LOG_PATTERN.match(filename).group(3)
                )
                if worker_pid_from_filename == pid:
                    return filename
        return None

    async def _resolve_actor_filename(
        self,
        actor_id: ActorID,
        get_actor_fn: Callable[[ActorID], Awaitable[Optional[ActorTableData]]],
        suffix: str,
        timeout: int,
    ):
        """
        Resolve actor log file
            Args:
                actor_id: The actor id.
                get_actor_fn: The function to get actor information.
                suffix: The suffix of the log file.
                timeout: Timeout in seconds.
            Returns:
                The log file name and node id.

            Raises:
                ValueError if actor data is not found or get_actor_fn is not provided.
        """
        if get_actor_fn is None:
            raise ValueError("get_actor_fn needs to be specified for actor_id")

        actor_data = await get_actor_fn(actor_id)
        if actor_data is None:
            raise ValueError(f"Actor ID {actor_id} not found.")
        # TODO(sang): Only the latest worker id can be obtained from
        # actor information now. That means, if actors are restarted,
        # there's no way for us to get the past worker ids.
        worker_id_binary = actor_data.address.worker_id
        if not worker_id_binary:
            raise ValueError(
                f"Worker ID for Actor ID {actor_id} not found. "
                "Actor is not scheduled yet."
            )
        worker_id = WorkerID(worker_id_binary)
        node_id_binary = actor_data.address.raylet_id
        if not node_id_binary:
            raise ValueError(
                f"Node ID for Actor ID {actor_id} not found. "
                "Actor is not scheduled yet."
            )
        node_id = NodeID(node_id_binary)
        log_filename = await self._resolve_worker_file(
            node_id_hex=node_id.hex(),
            worker_id_hex=worker_id.hex(),
            pid=None,
            suffix=suffix,
            timeout=timeout,
        )
        return node_id.hex(), log_filename

    async def _resolve_task_filename(
        self, task_id: str, attempt_number: int, suffix: str, timeout: int
    ):
        """
        Resolve log file for a task.

        Args:
            task_id: The task id.
            attempt_number: The attempt number.
            suffix: The suffix of the log file, e.g. out or err
            timeout: Timeout in seconds.

        Returns:
            The log file name, node id, the start and end offsets of the
            corresponding task log in the file.

        Raises:
            FileNotFoundError if the log file is not found.
            ValueError if the suffix is not out or err.

        """
        log_filename = None
        node_id = None
        start_offset = None
        end_offset = None

        if suffix not in ["out", "err"]:
            raise ValueError(f"Suffix {suffix} is not supported.")

        reply = await self.client.get_all_task_info(
            filters=[("task_id", "=", task_id)], timeout=timeout
        )
        # Check if the task is found.
        if len(reply.events_by_task) == 0:
            raise FileNotFoundError(
                f"Could not find log file for task: {task_id}"
                f" (attempt {attempt_number}) with suffix: {suffix}"
            )
        task_event = None
        for t in reply.events_by_task:
            if t.attempt_number == attempt_number:
                task_event = t
                break

        if task_event is None:
            raise FileNotFoundError(
                "Could not find log file for task attempt:"
                f"{task_id}({attempt_number})"
            )
        # Get the worker id and node id.
        task = protobuf_to_task_state_dict(task_event)

        worker_id = task.get("worker_id", None)
        node_id = task.get("node_id", None)
        log_info = task.get("task_log_info", None)
        actor_id = task.get("actor_id", None)

        if node_id is None:
            raise FileNotFoundError(
                "Could not find log file for task attempt."
                f"{task_id}({attempt_number}) due to missing node info."
            )

        if log_info is None and actor_id is not None:
            # This is a concurrent actor task. The logs will be interleaved.
            # So we return the log file of the actor instead.
            raise FileNotFoundError(
                f"For actor task, please query actor log for "
                f"actor({actor_id}): e.g. ray logs actor --id {actor_id} . Or "
                "set RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1 in actor's runtime env "
                "or when starting the cluster. Recording actor task's log could be "
                "expensive, so Ray turns it off by default."
            )
        elif log_info is None:
            raise FileNotFoundError(
                "Could not find log file for task attempt:"
                f"{task_id}({attempt_number})."
                f"Worker id = {worker_id}, node id = {node_id},"
                f"log_info = {log_info}"
            )

        filename_key = "stdout_file" if suffix == "out" else "stderr_file"
        log_filename = log_info.get(filename_key, None)
        if log_filename is None:
            raise FileNotFoundError(
                f"Missing log filename info in {log_info} for task {task_id},"
                f"attempt {attempt_number}"
            )

        start_offset = log_info.get(f"std{suffix}_start", None)
        end_offset = log_info.get(f"std{suffix}_end", None)

        return node_id, log_filename, start_offset, end_offset

    async def resolve_filename(
        self,
        *,
        node_id: Optional[str] = None,
        log_filename: Optional[str] = None,
        actor_id: Optional[str] = None,
        task_id: Optional[str] = None,
        attempt_number: Optional[int] = None,
        pid: Optional[str] = None,
        get_actor_fn: Optional[
            Callable[[ActorID], Awaitable[Optional[ActorTableData]]]
        ] = None,
        timeout: int = DEFAULT_RPC_TIMEOUT,
        suffix: str = "out",
        submission_id: Optional[str] = None,
    ) -> ResolvedStreamFileInfo:
        """Return the file name given all options.

        Args:
            node_id: The node's id from which logs are resolved.
            log_filename: Filename of the log file.
            actor_id: Id of the actor that generates the log file.
            task_id: Id of the task that generates the log file.
            pid: Id of the worker process that generates the log file.
            get_actor_fn: Callback to get the actor's data by id.
            timeout: Timeout for the gRPC to listing logs on the node
                specified by `node_id`.
            suffix: Log suffix if no `log_filename` is provided, when
                resolving by other ids'. Default to "out".
            submission_id: The submission id for a submission job.
        """
        start_offset = None
        end_offset = None
        if suffix not in ["out", "err"]:
            raise ValueError(f"Suffix {suffix} is not supported. ")

        # TODO(rickyx): We should make sure we do some sort of checking on the log
        # filename
        if actor_id:
            node_id, log_filename = await self._resolve_actor_filename(
                ActorID.from_hex(actor_id), get_actor_fn, suffix, timeout
            )

        elif task_id:
            (
                node_id,
                log_filename,
                start_offset,
                end_offset,
            ) = await self._resolve_task_filename(
                task_id, attempt_number, suffix, timeout
            )

        elif submission_id:
            node_id, log_filename = await self._resolve_job_filename(submission_id)

        elif pid:
            if node_id is None:
                raise ValueError(
                    "Node id needs to be specified for resolving"
                    f" filenames of pid {pid}"
                )
            log_filename = await self._resolve_worker_file(
                node_id_hex=node_id,
                worker_id_hex=None,
                pid=pid,
                suffix=suffix,
                timeout=timeout,
            )

        if log_filename is None:
            raise FileNotFoundError(
                "Could not find a log file. Please make sure the given "
                "option exists in the cluster.\n"
                f"\tnode_id: {node_id}\n"
                f"\tfilename: {log_filename}\n"
                f"\tactor_id: {actor_id}\n"
                f"\ttask_id: {task_id}\n"
                f"\tpid: {pid}\n"
                f"\tsuffix: {suffix}\n"
                f"\tsubmission_id: {submission_id}\n"
                f"\tattempt_number: {attempt_number}\n"
            )

        res = ResolvedStreamFileInfo(
            node_id=node_id,
            filename=log_filename,
            start_offset=start_offset,
            end_offset=end_offset,
        )
        logger.info(f"Resolved log file: {res}")
        return res

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
