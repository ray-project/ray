import asyncio
import copy
import json
import logging
import os
import psutil
import random
import signal
import string
import subprocess
import sys
import time
import traceback
from asyncio.tasks import FIRST_COMPLETED
from collections import deque
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    SchedulingStrategyT,
)
import ray
from ray._private.gcs_utils import GcsAioClient
from ray._private.utils import run_background_task
import ray._private.ray_constants as ray_constants
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR
from ray.actor import ActorHandle
from ray.dashboard.consts import RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR
from ray.dashboard.modules.job.common import (
    JOB_ID_METADATA_KEY,
    JOB_NAME_METADATA_KEY,
    JOB_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
    JobInfo,
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.utils import file_tail_iterator
from ray.exceptions import ActorUnschedulableError, RuntimeEnvSetupError
from ray.job_submission import JobStatus
from ray._private.event.event_logger import get_event_logger
from ray.core.generated.event_pb2 import Event

logger = logging.getLogger(__name__)

# asyncio python version compatibility
try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

# Windows requires additional packages for proper process control.
if sys.platform == "win32":
    try:
        import win32api
        import win32con
        import win32job
    except (ModuleNotFoundError, ImportError) as e:
        win32api = None
        win32con = None
        win32job = None
        logger.warning(
            "Failed to Import win32api. For best usage experience run "
            f"'conda install pywin32'. Import error: {e}"
        )


def generate_job_id() -> str:
    """Returns a job_id of the form 'raysubmit_XYZ'.

    Prefixed with 'raysubmit' to avoid confusion with Ray JobID (driver ID).
    """
    rand = random.SystemRandom()
    possible_characters = list(
        set(string.ascii_letters + string.digits)
        - {"I", "l", "o", "O", "0"}  # No confusing characters
    )
    id_part = "".join(rand.choices(possible_characters, k=16))
    return f"raysubmit_{id_part}"


class JobLogStorageClient:
    """
    Disk storage for stdout / stderr of driver script logs.
    """

    JOB_LOGS_PATH = "job-driver-{job_id}.log"
    # Number of last N lines to put in job message upon failure.
    NUM_LOG_LINES_ON_ERROR = 10
    # Maximum number of characters to print out of the logs to avoid
    # HUGE log outputs that bring down the api server
    MAX_LOG_SIZE = 20000

    def get_logs(self, job_id: str) -> str:
        try:
            with open(self.get_log_file_path(job_id), "r") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def tail_logs(self, job_id: str) -> Iterator[List[str]]:
        return file_tail_iterator(self.get_log_file_path(job_id))

    def get_last_n_log_lines(
        self, job_id: str, num_log_lines=NUM_LOG_LINES_ON_ERROR
    ) -> str:
        """
        Returns the last MAX_LOG_SIZE (20000) characters in the last
        `num_log_lines` lines.

        Args:
            job_id: The id of the job whose logs we want to return
            num_log_lines: The number of lines to return.
        """
        log_tail_iter = self.tail_logs(job_id)
        log_tail_deque = deque(maxlen=num_log_lines)
        for lines in log_tail_iter:
            if lines is None:
                break
            else:
                # log_tail_iter can return batches of lines at a time.
                for line in lines:
                    log_tail_deque.append(line)

        return "".join(log_tail_deque)[-self.MAX_LOG_SIZE :]

    def get_log_file_path(self, job_id: str) -> Tuple[str, str]:
        """
        Get the file path to the logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.log
        """
        return os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            self.JOB_LOGS_PATH.format(job_id=job_id),
        )


class JobSupervisor:
    """
    Ray actor created by JobManager for each submitted job, responsible to
    setup runtime_env, execute given shell command in subprocess, update job
    status, persist job logs and manage subprocess group cleaning.

    One job supervisor actor maps to one subprocess, for one job_id.
    Job supervisor actor should fate share with subprocess it created.
    """

    DEFAULT_RAY_JOB_STOP_WAIT_TIME_S = 3
    SUBPROCESS_POLL_PERIOD_S = 0.1
    VALID_STOP_SIGNALS = ["SIGINT", "SIGTERM"]

    def __init__(
        self,
        job_id: str,
        entrypoint: str,
        user_metadata: Dict[str, str],
        gcs_address: str,
    ):
        self._job_id = job_id
        gcs_aio_client = GcsAioClient(address=gcs_address)
        self._job_info_client = JobInfoStorageClient(gcs_aio_client)
        self._log_client = JobLogStorageClient()
        self._entrypoint = entrypoint

        # Default metadata if not passed by the user.
        self._metadata = {JOB_ID_METADATA_KEY: job_id, JOB_NAME_METADATA_KEY: job_id}
        self._metadata.update(user_metadata)

        # fire and forget call from outer job manager to this actor
        self._stop_event = asyncio.Event()

        # Windows Job Object used to handle stopping the child processes.
        self._win32_job_object = None

    def _get_driver_runtime_env(
        self, resources_specified: bool = False
    ) -> Dict[str, Any]:
        """Get the runtime env that should be set in the job driver.

        Args:
            resources_specified: Whether the user specified resources (CPUs, GPUs,
                custom resources) in the submit_job request. If so, we will skip
                the workaround for GPU detection introduced in #24546, so that the
                behavior matches that of the user specifying resources for any
                other actor.

        Returns:
            The runtime env that should be set in the job driver.
        """
        # Get the runtime_env set for the supervisor actor.
        curr_runtime_env = dict(ray.get_runtime_context().runtime_env)
        if resources_specified:
            return curr_runtime_env
        # Allow CUDA_VISIBLE_DEVICES to be set normally for the driver's tasks
        # & actors.
        env_vars = curr_runtime_env.get("env_vars", {})
        env_vars.pop(ray_constants.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR)
        curr_runtime_env["env_vars"] = env_vars
        return curr_runtime_env

    def ping(self):
        """Used to check the health of the actor."""
        pass

    def _exec_entrypoint(self, logs_path: str) -> subprocess.Popen:
        """
        Runs the entrypoint command as a child process, streaming stderr &
        stdout to given log files.

        Unix systems:
        Meanwhile we start a demon process and group driver
        subprocess in same pgid, such that if job actor dies, entire process
        group also fate share with it.

        Windows systems:
        A jobObject is created to enable fate sharing for the entire process group.

        Args:
            logs_path: File path on head node's local disk to store driver
                command's stdout & stderr.
        Returns:
            child_process: Child process that runs the driver command. Can be
                terminated or killed upon user calling stop().
        """
        with open(logs_path, "w") as logs_file:
            child_process = subprocess.Popen(
                self._entrypoint,
                shell=True,
                start_new_session=True,
                stdout=logs_file,
                stderr=subprocess.STDOUT,
                # Ray intentionally blocks SIGINT in all processes, so if the user wants
                # to stop job through SIGINT, we need to unblock it in the child process
                preexec_fn=lambda: signal.pthread_sigmask(
                    signal.SIG_UNBLOCK, {signal.SIGINT}
                )
                if sys.platform != "win32"
                and os.environ.get("RAY_JOB_STOP_SIGNAL") == "SIGINT"
                else None,
            )
            parent_pid = os.getpid()
            child_pid = child_process.pid
            # Create new pgid with new subprocess to execute driver command

            if sys.platform != "win32":
                try:
                    child_pgid = os.getpgid(child_pid)
                except ProcessLookupError:
                    # Process died before we could get its pgid.
                    return child_process

                # Open a new subprocess to kill the child process when the parent
                # process dies kill -s 0 parent_pid will succeed if the parent is
                # alive. If it fails, SIGKILL the child process group and exit
                subprocess.Popen(
                    f"while kill -s 0 {parent_pid}; do sleep 1; done; kill -9 -{child_pgid}",  # noqa: E501
                    shell=True,
                    # Suppress output
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

            elif sys.platform == "win32" and win32api:
                # Create a JobObject to which the child process (and its children)
                # will be connected. This job object can be used to kill the child
                # processes explicitly or when the jobObject gets deleted during
                # garbage collection.
                self._win32_job_object = win32job.CreateJobObject(None, "")
                win32_job_info = win32job.QueryInformationJobObject(
                    self._win32_job_object, win32job.JobObjectExtendedLimitInformation
                )
                win32_job_info["BasicLimitInformation"][
                    "LimitFlags"
                ] = win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                win32job.SetInformationJobObject(
                    self._win32_job_object,
                    win32job.JobObjectExtendedLimitInformation,
                    win32_job_info,
                )
                child_handle = win32api.OpenProcess(
                    win32con.PROCESS_TERMINATE | win32con.PROCESS_SET_QUOTA,
                    False,
                    child_pid,
                )
                win32job.AssignProcessToJobObject(self._win32_job_object, child_handle)

            return child_process

    def _get_driver_env_vars(self, resources_specified: bool) -> Dict[str, str]:
        """Returns environment variables that should be set in the driver."""
        # RAY_ADDRESS may be the dashboard URL but not the gcs address,
        # so when the environment variable is not empty, we force set RAY_ADDRESS
        # to "auto" to avoid function `canonicalize_bootstrap_address_or_die` returning
        # the wrong GCS address.
        # TODO(Jialing He, Archit Kulkarni): Definition of Specification RAY_ADDRESS
        if ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE in os.environ:
            os.environ[ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE] = "auto"
        ray_addr = ray._private.services.canonicalize_bootstrap_address_or_die(
            "auto", ray.worker._global_node._ray_params.temp_dir
        )
        assert ray_addr is not None
        return {
            # Set JobConfig for the child process (runtime_env, metadata).
            RAY_JOB_CONFIG_JSON_ENV_VAR: json.dumps(
                {
                    "runtime_env": self._get_driver_runtime_env(resources_specified),
                    "metadata": self._metadata,
                }
            ),
            # Always set RAY_ADDRESS as find_bootstrap_address address for
            # job submission. In case of local development, prevent user from
            # re-using http://{address}:{dashboard_port} to interact with
            # jobs SDK.
            # TODO:(mwtian) Check why "auto" does not work in entrypoint script
            ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE: ray_addr,
            # Set PYTHONUNBUFFERED=1 to stream logs during the job instead of
            # only streaming them upon completion of the job.
            "PYTHONUNBUFFERED": "1",
        }

    async def _polling(self, child_process: subprocess.Popen) -> int:
        while child_process is not None:
            return_code = child_process.poll()
            if return_code is not None:
                # subprocess finished with return code
                return return_code
            else:
                # still running, yield control, 0.1s by default
                await asyncio.sleep(self.SUBPROCESS_POLL_PERIOD_S)

    async def _poll_all(self, processes: List[psutil.Process]):
        """Poll processes until all are completed."""
        while True:
            (_, alive) = psutil.wait_procs(processes, timeout=0)
            if len(alive) == 0:
                return
            else:
                await asyncio.sleep(self.SUBPROCESS_POLL_PERIOD_S)

    def _kill_processes(self, processes: List[psutil.Process], sig: signal.Signals):
        """Ensures each process is already finished or send a kill signal."""
        for proc in processes:
            try:
                os.kill(proc.pid, sig)
            except ProcessLookupError:
                # Process is already dead
                pass

    async def run(
        self,
        # Signal actor used in testing to capture PENDING -> RUNNING cases
        _start_signal_actor: Optional[ActorHandle] = None,
        resources_specified: bool = False,
    ):
        """
        Stop and start both happen asynchrously, coordinated by asyncio event
        and coroutine, respectively.

        1) Sets job status as running
        2) Pass runtime env and metadata to subprocess as serialized env
            variables.
        3) Handle concurrent events of driver execution and
        """
        curr_status = await self._job_info_client.get_status(self._job_id)
        assert curr_status == JobStatus.PENDING, "Run should only be called once."

        if _start_signal_actor:
            # Block in PENDING state until start signal received.
            await _start_signal_actor.wait.remote()

        driver_agent_http_address = (
            "http://"
            f"{ray.worker.global_worker.node.node_ip_address}:"
            f"{ray.worker.global_worker.node.dashboard_agent_listen_port}"
        )
        driver_node_id = ray.worker.global_worker.current_node_id.hex()

        await self._job_info_client.put_status(
            self._job_id,
            JobStatus.RUNNING,
            jobinfo_replace_kwargs={
                "driver_agent_http_address": driver_agent_http_address,
                "driver_node_id": driver_node_id,
            },
        )

        try:
            # Configure environment variables for the child process. These
            # will *not* be set in the runtime_env, so they apply to the driver
            # only, not its tasks & actors.
            os.environ.update(self._get_driver_env_vars(resources_specified))
            logger.info(
                "Submitting job with RAY_ADDRESS = "
                f"{os.environ[ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE]}"
            )
            log_path = self._log_client.get_log_file_path(self._job_id)
            child_process = self._exec_entrypoint(log_path)
            child_pid = child_process.pid

            polling_task = create_task(self._polling(child_process))
            finished, _ = await asyncio.wait(
                [polling_task, self._stop_event.wait()], return_when=FIRST_COMPLETED
            )

            if self._stop_event.is_set():
                polling_task.cancel()
                if sys.platform == "win32" and self._win32_job_object:
                    win32job.TerminateJobObject(self._win32_job_object, -1)
                elif sys.platform != "win32":
                    stop_signal = os.environ.get("RAY_JOB_STOP_SIGNAL", "SIGTERM")
                    if stop_signal not in self.VALID_STOP_SIGNALS:
                        logger.warning(
                            f"{stop_signal} not a valid stop signal. Terminating "
                            "job with SIGTERM."
                        )
                        stop_signal = "SIGTERM"
                    
                    job_process = psutil.Process(child_pid)
                    proc_to_kill = [job_process] + job_process.children(recursive=True)

                    # Send stop signal and wait for job to terminate gracefully, 
                    # otherwise SIGKILL job forcefully after timeout.
                    self._kill_processes(proc_to_kill, getattr(signal, stop_signal))
                    try:
                        stop_job_wait_time = int(
                            os.environ.get(
                                "RAY_JOB_STOP_WAIT_TIME_S",
                                self.DEFAULT_RAY_JOB_STOP_WAIT_TIME_S,
                            )
                        )
                        poll_job_stop_task = create_task(self._poll_all(proc_to_kill))
                        await asyncio.wait_for(poll_job_stop_task, stop_job_wait_time)
                        logger.info(
                            f"Job {self._job_id} has been terminated gracefully "
                            f"with {stop_signal}."
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"Attempt to gracefully terminate job {self._job_id} "
                            f"through {stop_signal} has timed out after "
                            f"{stop_job_wait_time} seconds. Job is now being "
                            "force-killed."
                        )
                        self._kill_processes(proc_to_kill, signal.SIGKILL)
                await self._job_info_client.put_status(self._job_id, JobStatus.STOPPED)
            else:
                # Child process finished execution and no stop event is set
                # at the same time
                assert len(finished) == 1, "Should have only one coroutine done"
                [child_process_task] = finished
                return_code = child_process_task.result()
                if return_code == 0:
                    await self._job_info_client.put_status(
                        self._job_id, JobStatus.SUCCEEDED
                    )
                else:
                    log_tail = self._log_client.get_last_n_log_lines(self._job_id)
                    if log_tail is not None and log_tail != "":
                        message = (
                            "Job failed due to an application error, "
                            "last available logs (truncated to 20,000 chars):\n"
                            + log_tail
                        )
                    else:
                        message = None
                    await self._job_info_client.put_status(
                        self._job_id, JobStatus.FAILED, message=message
                    )
        except Exception:
            logger.error(
                "Got unexpected exception while trying to execute driver "
                f"command. {traceback.format_exc()}"
            )
            try:
                await self._job_info_client.put_status(
                    self._job_id, JobStatus.FAILED, message=traceback.format_exc()
                )
            except Exception:
                logger.error(
                    "Failed to update job status to FAILED. "
                    f"Exception: {traceback.format_exc()}"
                )
        finally:
            # clean up actor after tasks are finished
            ray.actor.exit_actor()

    def stop(self):
        """Set step_event and let run() handle the rest in its asyncio.wait()."""
        self._stop_event.set()


class JobManager:
    """Provide python APIs for job submission and management.

    It does not provide persistence, all info will be lost if the cluster
    goes down.
    """

    # Time that we will sleep while tailing logs if no new log line is
    # available.
    LOG_TAIL_SLEEP_S = 1
    JOB_MONITOR_LOOP_PERIOD_S = 1
    WAIT_FOR_ACTOR_DEATH_TIMEOUT_S = 0.1

    def __init__(self, gcs_aio_client: GcsAioClient, logs_dir: str):
        self._gcs_aio_client = gcs_aio_client
        self._job_info_client = JobInfoStorageClient(gcs_aio_client)
        self._gcs_address = gcs_aio_client._channel._gcs_address
        self._log_client = JobLogStorageClient()
        self._supervisor_actor_cls = ray.remote(JobSupervisor)
        self.monitored_jobs = set()
        try:
            self.event_logger = get_event_logger(Event.SourceType.JOBS, logs_dir)
        except Exception:
            self.event_logger = None

        run_background_task(self._recover_running_jobs())

    async def _recover_running_jobs(self):
        """Recovers all running jobs from the status client.

        For each job, we will spawn a coroutine to monitor it.
        Each will be added to self._running_jobs and reconciled.
        """
        all_jobs = await self._job_info_client.get_all_jobs()
        for job_id, job_info in all_jobs.items():
            if not job_info.status.is_terminal():
                run_background_task(self._monitor_job(job_id))

    def _get_actor_for_job(self, job_id: str) -> Optional[ActorHandle]:
        try:
            return ray.get_actor(
                JOB_ACTOR_NAME_TEMPLATE.format(job_id=job_id),
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
            )
        except ValueError:  # Ray returns ValueError for nonexistent actor.
            return None

    async def _monitor_job(
        self, job_id: str, job_supervisor: Optional[ActorHandle] = None
    ):
        """Monitors the specified job until it enters a terminal state.

        This is necessary because we need to handle the case where the
        JobSupervisor dies unexpectedly.
        """
        if job_id in self.monitored_jobs:
            logger.debug(f"Job {job_id} is already being monitored.")
            return

        self.monitored_jobs.add(job_id)
        try:
            await self._monitor_job_internal(job_id, job_supervisor)
        finally:
            self.monitored_jobs.remove(job_id)

    async def _monitor_job_internal(
        self, job_id: str, job_supervisor: Optional[ActorHandle] = None
    ):
        is_alive = True
        if job_supervisor is None:
            job_supervisor = self._get_actor_for_job(job_id)

            if job_supervisor is None:
                logger.error(f"Failed to get job supervisor for job {job_id}.")
                await self._job_info_client.put_status(
                    job_id,
                    JobStatus.FAILED,
                    message="Unexpected error occurred: Failed to get job supervisor.",
                )
                is_alive = False

        while is_alive:
            try:
                await job_supervisor.ping.remote()
                await asyncio.sleep(self.JOB_MONITOR_LOOP_PERIOD_S)
            except Exception as e:
                is_alive = False
                job_status = await self._job_info_client.get_status(job_id)
                job_error_message = None
                if job_status == JobStatus.FAILED:
                    job_error_message = (
                        "See more details from the dashboard "
                        "`Job` page or the state API `ray list jobs`."
                    )

                if job_status.is_terminal():
                    # If the job is already in a terminal state, then the actor
                    # exiting is expected.
                    pass
                elif isinstance(e, RuntimeEnvSetupError):
                    logger.info(f"Failed to set up runtime_env for job {job_id}.")
                    job_error_message = f"runtime_env setup failed: {e}"
                    job_status = JobStatus.FAILED
                    await self._job_info_client.put_status(
                        job_id,
                        job_status,
                        message=job_error_message,
                    )
                elif isinstance(e, ActorUnschedulableError):
                    logger.info(
                        f"Failed to schedule job {job_id} because the supervisor actor "
                        f"could not be scheduled: {e}"
                    )
                    await self._job_info_client.put_status(
                        job_id,
                        JobStatus.FAILED,
                        message=(f"Job supervisor actor could not be scheduled: {e}"),
                    )
                else:
                    logger.warning(
                        f"Job supervisor for job {job_id} failed unexpectedly: {e}."
                    )
                    job_error_message = f"Unexpected error occurred: {e}"
                    job_status = JobStatus.FAILED
                    await self._job_info_client.put_status(
                        job_id,
                        job_status,
                        message=job_error_message,
                    )

                # Log events
                if self.event_logger:
                    event_log = (
                        f"Completed a ray job {job_id} with a status {job_status}."
                    )
                    if job_error_message:
                        event_log += f" {job_error_message}"
                        self.event_logger.error(event_log, submission_id=job_id)
                    else:
                        self.event_logger.info(event_log, submission_id=job_id)

        # Kill the actor defensively to avoid leaking actors in unexpected error cases.
        if job_supervisor is not None:
            ray.kill(job_supervisor, no_restart=True)

    def _get_current_node_resource_key(self) -> str:
        """Get the Ray resource key for current node.

        It can be used for actor placement.
        """
        current_node_id = ray.get_runtime_context().node_id.hex()
        for node in ray.nodes():
            if node["NodeID"] == current_node_id:
                # Found the node.
                for key in node["Resources"].keys():
                    if key.startswith("node:"):
                        return key
        else:
            raise ValueError("Cannot find the node dictionary for current node.")

    def _handle_supervisor_startup(self, job_id: str, result: Optional[Exception]):
        """Handle the result of starting a job supervisor actor.

        If started successfully, result should be None. Otherwise it should be
        an Exception.

        On failure, the job will be marked failed with a relevant error
        message.
        """
        if result is None:
            return

    def _get_supervisor_runtime_env(
        self, user_runtime_env: Dict[str, Any], resources_specified: bool = False
    ) -> Dict[str, Any]:
        """Configure and return the runtime_env for the supervisor actor.

        Args:
            user_runtime_env: The runtime_env specified by the user.
            resources_specified: Whether the user specified resources in the
                submit_job() call. If so, we will skip the workaround introduced
                in #24546 for GPU detection and just use the user's resource
                requests, so that the behavior matches that of the user specifying
                resources for any other actor.

        Returns:
            The runtime_env for the supervisor actor.
        """
        # Make a copy to avoid mutating passed runtime_env.
        runtime_env = (
            copy.deepcopy(user_runtime_env) if user_runtime_env is not None else {}
        )

        # NOTE(edoakes): Can't use .get(, {}) here because we need to handle the case
        # where env_vars is explicitly set to `None`.
        env_vars = runtime_env.get("env_vars")
        if env_vars is None:
            env_vars = {}

        if not resources_specified:
            # Don't set CUDA_VISIBLE_DEVICES for the supervisor actor so the
            # driver can use GPUs if it wants to. This will be removed from
            # the driver's runtime_env so it isn't inherited by tasks & actors.
            env_vars[ray_constants.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR] = "1"
        runtime_env["env_vars"] = env_vars
        return runtime_env

    async def _get_scheduling_strategy(
        self, resources_specified: bool
    ) -> SchedulingStrategyT:
        """Get the scheduling strategy for the job.

        If resources_specified is true, or if the environment variable is set to
        allow the job to run on worker nodes, we will use Ray's default actor
        placement strategy. Otherwise, we will force the job to use the head node.

        Args:
            resources_specified: Whether the job specified any resources
                (CPUs, GPUs, or custom resources).

        Returns:
            The scheduling strategy to use for the job.
        """
        if resources_specified:
            return "DEFAULT"

        if os.environ.get(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "0") == "1":
            logger.info(
                f"{RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR} was set to 1. "
                "Using Ray's default actor scheduling strategy for the job "
                "driver instead of running it on the head node."
            )
            return "DEFAULT"

        # If the user did not specify any resources or set the driver on worker nodes
        # env var, we will run the driver on the head node.

        head_node_id_bytes = await self._gcs_aio_client.internal_kv_get(
            "head_node_id".encode(),
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=30,
        )
        if head_node_id_bytes is None:
            logger.info(
                "Head node ID not found in GCS. Using Ray's default actor "
                "scheduling strategy for the job driver instead of running "
                "it on the head node."
            )
            scheduling_strategy = "DEFAULT"
        else:
            head_node_id = head_node_id_bytes.decode()
            logger.info(
                "Head node ID found in GCS; scheduling job driver on "
                f"head node {head_node_id}"
            )
            scheduling_strategy = NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        return scheduling_strategy

    async def submit_job(
        self,
        *,
        entrypoint: str,
        submission_id: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
        entrypoint_num_cpus: Optional[Union[int, float]] = None,
        entrypoint_num_gpus: Optional[Union[int, float]] = None,
        entrypoint_resources: Optional[Dict[str, float]] = None,
        _start_signal_actor: Optional[ActorHandle] = None,
    ) -> str:
        """
        Job execution happens asynchronously.

        1) Generate a new unique id for this job submission, each call of this
            method assumes they're independent submission with its own new
            ID, job supervisor actor, and child process.
        2) Create new detached actor with same runtime_env as job spec

        Actual setting up runtime_env, subprocess group, driver command
        execution, subprocess cleaning up and running status update to GCS
        is all handled by job supervisor actor.

        Args:
            entrypoint: Driver command to execute in subprocess shell.
                Represents the entrypoint to start user application.
            runtime_env: Runtime environment used to execute driver command,
                which could contain its own ray.init() to configure runtime
                env at ray cluster, task and actor level.
            metadata: Support passing arbitrary data to driver command in
                case needed.
            entrypoint_num_cpus: The quantity of CPU cores to reserve for the execution
                of the entrypoint command, separately from any tasks or actors launched
                by it. Defaults to 0.
            entrypoint_num_gpus: The quantity of GPUs to reserve for
                the entrypoint command, separately from any tasks or actors launched
                by it. Defaults to 0.
            entrypoint_resources: The quantity of various custom resources
                to reserve for the entrypoint command, separately from any tasks or
                actors launched by it.
            _start_signal_actor: Used in testing only to capture state
                transitions between PENDING -> RUNNING. Regular user shouldn't
                need this.

        Returns:
            job_id: Generated uuid for further job management. Only valid
                within the same ray cluster.
        """
        if entrypoint_num_cpus is None:
            entrypoint_num_cpus = 0
        if entrypoint_num_gpus is None:
            entrypoint_num_gpus = 0
        if submission_id is None:
            submission_id = generate_job_id()
        elif await self._job_info_client.get_status(submission_id) is not None:
            raise RuntimeError(f"Job {submission_id} already exists.")

        logger.info(f"Starting job with submission_id: {submission_id}")
        job_info = JobInfo(
            entrypoint=entrypoint,
            status=JobStatus.PENDING,
            start_time=int(time.time() * 1000),
            metadata=metadata,
            runtime_env=runtime_env,
            entrypoint_num_cpus=entrypoint_num_cpus,
            entrypoint_num_gpus=entrypoint_num_gpus,
            entrypoint_resources=entrypoint_resources,
        )
        await self._job_info_client.put_info(submission_id, job_info)

        # Wait for the actor to start up asynchronously so this call always
        # returns immediately and we can catch errors with the actor starting
        # up.
        try:
            resources_specified = any(
                [
                    entrypoint_num_cpus is not None and entrypoint_num_cpus > 0,
                    entrypoint_num_gpus is not None and entrypoint_num_gpus > 0,
                    entrypoint_resources not in [None, {}],
                ]
            )
            scheduling_strategy = await self._get_scheduling_strategy(
                resources_specified
            )
            if self.event_logger:
                self.event_logger.info(
                    f"Started a ray job {submission_id}.", submission_id=submission_id
                )
            supervisor = self._supervisor_actor_cls.options(
                lifetime="detached",
                name=JOB_ACTOR_NAME_TEMPLATE.format(job_id=submission_id),
                num_cpus=entrypoint_num_cpus,
                num_gpus=entrypoint_num_gpus,
                resources=entrypoint_resources,
                scheduling_strategy=scheduling_strategy,
                runtime_env=self._get_supervisor_runtime_env(
                    runtime_env, resources_specified
                ),
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
            ).remote(submission_id, entrypoint, metadata or {}, self._gcs_address)
            supervisor.run.remote(
                _start_signal_actor=_start_signal_actor,
                resources_specified=resources_specified,
            )

            # Monitor the job in the background so we can detect errors without
            # requiring a client to poll.
            run_background_task(
                self._monitor_job(submission_id, job_supervisor=supervisor)
            )
        except Exception as e:
            await self._job_info_client.put_status(
                submission_id,
                JobStatus.FAILED,
                message=f"Failed to start Job Supervisor actor: {e}.",
            )

        return submission_id

    def stop_job(self, job_id) -> bool:
        """Request a job to exit, fire and forget.

        Returns whether or not the job was running.
        """
        job_supervisor_actor = self._get_actor_for_job(job_id)
        if job_supervisor_actor is not None:
            # Actor is still alive, signal it to stop the driver, fire and
            # forget
            job_supervisor_actor.stop.remote()
            return True
        else:
            return False

    async def delete_job(self, job_id):
        """Delete a job's info and metadata from the cluster."""
        job_status = await self._job_info_client.get_status(job_id)

        if job_status is None or not job_status.is_terminal():
            raise RuntimeError(
                f"Attempted to delete job '{job_id}', "
                f"but it is in a non-terminal state {job_status}."
            )

        await self._job_info_client.delete_info(job_id)
        return True

    def job_info_client(self) -> JobInfoStorageClient:
        return self._job_info_client

    async def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get latest status of a job."""
        return await self._job_info_client.get_status(job_id)

    async def get_job_info(self, job_id: str) -> Optional[JobInfo]:
        """Get latest info of a job."""
        return await self._job_info_client.get_info(job_id)

    async def list_jobs(self) -> Dict[str, JobInfo]:
        """Get info for all jobs."""
        return await self._job_info_client.get_all_jobs()

    def get_job_logs(self, job_id: str) -> str:
        """Get all logs produced by a job."""
        return self._log_client.get_logs(job_id)

    async def tail_job_logs(self, job_id: str) -> Iterator[str]:
        """Return an iterator following the logs of a job."""
        if await self.get_job_status(job_id) is None:
            raise RuntimeError(f"Job '{job_id}' does not exist.")

        for lines in self._log_client.tail_logs(job_id):
            if lines is None:
                # Return if the job has exited and there are no new log lines.
                status = await self.get_job_status(job_id)
                if status.is_terminal():
                    return

                await asyncio.sleep(self.LOG_TAIL_SLEEP_S)
            else:
                yield "".join(lines)
