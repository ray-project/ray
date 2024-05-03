import asyncio
import copy
import json
import logging
import os
import signal
import subprocess
import sys
import time
import traceback
from asyncio.tasks import FIRST_COMPLETED
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union, Tuple

import psutil

import ray
import ray._private.ray_constants as ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR
from ray._private.utils import run_background_task
from ray.actor import ActorHandle
from ray.dashboard.consts import (
    RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR,
    RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG_ENV_VAR,
)
from ray.dashboard.modules.job.common import (
    JOB_ID_METADATA_KEY,
    JOB_NAME_METADATA_KEY,
    JOB_EXECUTOR_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
    JobInfoStorageClient,
    JobInfo,
)
from ray.dashboard.modules.job.job_log_storage_client import JobLogStorageClient
from ray.job_submission import JobStatus
from ray.runtime_env import RuntimeEnvConfig
from ray.util.scheduling_strategies import (
    SchedulingStrategyT, NodeAffinitySchedulingStrategy,
)

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

        logger = logging.getLogger(__name__)
        logger.warning(
            "Failed to Import win32api. For best usage experience run "
            f"'conda install pywin32'. Import error: {e}"
        )


class JobSupervisor:
    """
    Ray actor created by JobManager for each submitted job, responsible to
    setup runtime_env, execute given shell command in subprocess, update job
    status, persist job logs and manage subprocess group cleaning.

    One job supervisor actor maps to one subprocess, for one job_id.
    Job supervisor actor should fate share with subprocess it created.
    """

    def __init__(
        self,
        job_id: str,
        entrypoint: str,
        gcs_address: str,
    ):
        self._job_id = job_id
        self._entrypoint = entrypoint

        self._job_info_client = JobInfoStorageClient(GcsAioClient(address=gcs_address))

        self._runner_actor_cls = ray.remote(JobRunner)
        self._runner = None

    async def ping(self):
        """Pings job runner to make sure Ray job is being executed"""
        assert self._runner, "Runner is not initialized!"

        await self._runner.ping.remote()

    async def stop(self):
        """Proxies request to job runner"""
        assert self._runner, "Runner is not initialized!"
        # Stop the job runner actor & killing the driver process
        await self._runner.stop.remote()

    async def start(
        self,
        *,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
        entrypoint_num_cpus: Optional[Union[int, float]] = None,
        entrypoint_num_gpus: Optional[Union[int, float]] = None,
        entrypoint_memory: Optional[int] = None,
        entrypoint_resources: Optional[Dict[str, float]] = None,
        # Signal actor used in testing to capture PENDING -> RUNNING cases
        _start_signal_actor: Optional[ActorHandle] = None,
    ):
        logger.info(f"Starting job with submission_id: {self._job_id}")

        job_info = JobInfo(
            entrypoint=self._entrypoint,
            status=JobStatus.PENDING,
            start_time=int(time.time() * 1000),
            metadata=metadata,
            runtime_env=runtime_env,
            entrypoint_num_cpus=entrypoint_num_cpus,
            entrypoint_num_gpus=entrypoint_num_gpus,
            entrypoint_memory=entrypoint_memory,
            entrypoint_resources=entrypoint_resources,
        )

        new_key_added = await self._job_info_client.put_info(
            self._job_id, job_info, overwrite=False
        )
        if not new_key_added:
            raise ValueError(
                f"Job with submission_id {self._job_id} already exists. "
                "Please use a different submission_id."
            )

        runner, resources_specified = await self._create_runner_actor(
            runtime_env=runtime_env,
            metadata=metadata,
            entrypoint_num_cpus=entrypoint_num_cpus,
            entrypoint_num_gpus=entrypoint_num_gpus,
            entrypoint_memory=entrypoint_memory,
            entrypoint_resources=entrypoint_resources
        )

        self._runner = runner

        # TODO clean up
        if _start_signal_actor:
            # Block in PENDING state until start signal received.
            await _start_signal_actor.wait.remote()

        # Job driver (entrypoint) is executed in a synchronous fashion,
        # therefore is performed as a background operation updating Ray Job's
        # state asynchronously upon job's driver completing the execution
        run_background_task(
            self._execute_sync(
                runner,
                resources_specified=resources_specified,
            )
        )

        if self.event_logger:
            self.event_logger.info(
                f"Started a ray job {self._job_id}.", submission_id=self._job_id
            )

    async def _execute_sync(self, runner: ActorHandle, *, resources_specified: bool):
        message: Optional[str] = None
        status: JobStatus = JobStatus.FAILED
        exit_code: Optional[int] = None

        try:
            driver_node_info: JobDriverNodeInfo = await runner.get_node_info.remote()
            driver_agent_http_address = f"http://{driver_node_info.node_ip}:{driver_node_info.dashboard_agent_port}"

            # Mark the job as running
            await self._job_info_client.put_status(
                self._job_id,
                JobStatus.RUNNING,
                jobinfo_replace_kwargs={
                    "driver_agent_http_address": driver_agent_http_address,
                    "driver_node_id": driver_node_info.node_id,
                },
            )

            result: JobExecutionResult = await runner.execute_sync.remote(
                resources_specified=resources_specified,
            )

            exit_code = result.driver_exit_code
            message = result.message

            if result.stopped:
                status = JobStatus.STOPPED
            elif result.driver_exit_code == 0:
                status = JobStatus.SUCCEEDED
            else:
                status = JobStatus.FAILED

        except Exception:
            message = traceback.format_exc()
            status = JobStatus.FAILED
            exit_code = None

            logger.error(
                f"Got unexpected exception while executing Ray job {self._job_id}: {message}"
            )

        finally:
            await self._job_info_client.put_status(
                self._job_id,
                status,
                driver_exit_code=exit_code,
                message=message,
            )

    async def _create_runner_actor(
        self,
        *,
        runtime_env: Optional[Dict[str, Any]],
        metadata: Optional[Dict[str, str]],
        entrypoint_num_cpus: Optional[Union[int, float]],
        entrypoint_num_gpus: Optional[Union[int, float]],
        entrypoint_memory: Optional[int],
        entrypoint_resources: Optional[Dict[str, float]],
    ) -> Tuple[ActorHandle, bool]:
        resources_specified = any(
            [
                entrypoint_num_cpus is not None and entrypoint_num_cpus > 0,
                entrypoint_num_gpus is not None and entrypoint_num_gpus > 0,
                entrypoint_memory is not None and entrypoint_memory > 0,
                entrypoint_resources not in [None, {}],
            ]
        )

        scheduling_strategy = self._get_scheduling_strategy(resources_specified)

        runner = self._runner_actor_cls.options(
            name=JOB_EXECUTOR_ACTOR_NAME_TEMPLATE.format(job_id=self._job_id),
            num_cpus=entrypoint_num_cpus,
            num_gpus=entrypoint_num_gpus,
            memory=entrypoint_memory,
            resources=entrypoint_resources,
            scheduling_strategy=scheduling_strategy,
            runtime_env=self._get_supervisor_runtime_env(
                runtime_env, self._job_id, resources_specified
            ),
            namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
        ).remote(
            job_id=self._job_id,
            entrypoint=self._entrypoint,
            user_metadata=metadata or {}
        )

        return runner, resources_specified

    @staticmethod
    def _get_scheduling_strategy(resources_specified: bool) -> SchedulingStrategyT:
        """Get the scheduling strategy for the job.

        If resources_specified is true, or if the environment variable is set to
        allow the job's driver (entrypoint) to run on worker nodes, we will use Ray's
        default actor placement strategy. Otherwise, we will force the job to use the
        head node.

        Args:
            resources_specified: Whether the job specified any resources
                (CPUs, GPUs, or custom resources).

        Returns:
            The scheduling strategy to use for the job.
        """
        if resources_specified:
            return "DEFAULT"
        elif os.environ.get(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "0") == "1":
            logger.info(
                f"{RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR} was set to 1. "
                "Using Ray's default actor scheduling strategy for the job "
                "driver instead of running it on the head node."
            )
            return "DEFAULT"

        # If the user did not specify any resources or set the driver on worker nodes
        # env var, we will run the driver on the head node.
        return NodeAffinitySchedulingStrategy(node_id=ray.worker.global_worker.current_node_id.hex(), soft=True)

    def _get_supervisor_runtime_env(
        self,
        user_runtime_env: Dict[str, Any],
        submission_id: str,
        resources_specified: bool = False,
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

        env_vars[ray_constants.RAY_WORKER_NICENESS] = "0"

        if not resources_specified:
            # Don't set CUDA_VISIBLE_DEVICES for the supervisor actor so the
            # driver can use GPUs if it wants to. This will be removed from
            # the driver's runtime_env so it isn't inherited by tasks & actors.
            env_vars[ray_constants.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR] = "1"
        runtime_env["env_vars"] = env_vars

        if os.getenv(RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG_ENV_VAR, "0") == "1":
            config = runtime_env.get("config")
            # Empty fields may be set to None, so we need to check for None explicitly.
            if config is None:
                config = RuntimeEnvConfig()
            config["log_files"] = [JobLogStorageClient.get_log_file_path(submission_id)]
            runtime_env["config"] = config
        return runtime_env


@dataclass
class JobDriverNodeInfo:
    node_id: str
    node_ip: str
    dashboard_agent_port: int


@dataclass
class JobExecutionResult:
    driver_exit_code: int
    message: Optional[str] = None
    stopped: bool = False


class JobRunner:
    """
    TODO elaborate
    """

    DEFAULT_RAY_JOB_STOP_WAIT_TIME_S = 3
    SUBPROCESS_POLL_PERIOD_S = 0.1
    VALID_STOP_SIGNALS = ["SIGINT", "SIGTERM"]

    def __init__(
        self,
        job_id: str,
        entrypoint: str,
        user_metadata: Dict[str, str],
    ):
        self._job_id = job_id
        self._entrypoint = entrypoint

        self._log_client = JobLogStorageClient()

        # Default metadata if not passed by the user.
        self._metadata = {JOB_ID_METADATA_KEY: job_id, JOB_NAME_METADATA_KEY: job_id}
        self._metadata.update(user_metadata)

        # fire and forget call from outer job manager to this actor
        self._stop_event = asyncio.Event()

        # Windows Job Object used to handle stopping the child processes.
        self._win32_job_object = None

        # Logger object to persist JobSupervisor logs in separate file.
        self._logger = logging.getLogger(f"{__name__}.supervisor-{job_id}")
        self._configure_logger()

    def _configure_logger(self) -> None:
        """
        Configure self._logger object to write logs to file based on job
        submission ID and to console.
        """
        supervisor_log_file_name = os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            f"jobs/supervisor-{self._job_id}.log",
        )
        os.makedirs(os.path.dirname(supervisor_log_file_name), exist_ok=True)
        self._logger.addHandler(logging.StreamHandler())
        self._logger.addHandler(logging.FileHandler(supervisor_log_file_name))

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
        env_vars.pop(ray_constants.RAY_WORKER_NICENESS)
        curr_runtime_env["env_vars"] = env_vars
        return curr_runtime_env

    async def get_node_info(self):
        return JobDriverNodeInfo(
            node_id=ray.worker.global_worker.current_node_id.hex(),
            node_ip=ray.worker.global_worker.node.node_ip_address,
            dashboard_agent_port=ray.worker.global_worker.node.dashboard_agent_listen_port,
        )

    async def stop(self):
        """Set step_event and let run() handle the rest in its asyncio.wait()."""
        self._stop_event.set()

    async def ping(self):
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
        # Open in append mode to avoid overwriting runtime_env setup logs for the
        # supervisor actor, which are also written to the same file.
        with open(logs_path, "a") as logs_file:
            child_process = subprocess.Popen(
                self._entrypoint,
                shell=True,
                start_new_session=True,
                stdout=logs_file,
                stderr=subprocess.STDOUT,
                # Ray intentionally blocks SIGINT in all processes, so if the user wants
                # to stop job through SIGINT, we need to unblock it in the child process
                preexec_fn=(
                    lambda: signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
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
        """Ensure each process is already finished or send a kill signal."""
        for proc in processes:
            try:
                os.kill(proc.pid, sig)
            except ProcessLookupError:
                # Process is already dead
                pass

    async def execute_sync(
        self,
        resources_specified: bool = False,
    ) -> JobExecutionResult:
        """
        TODO update
        Stop and start both happen asynchronously, coordinated by asyncio event
        and coroutine, respectively.

        1) Sets job status as running
        2) Pass runtime env and metadata to subprocess as serialized env
            variables.
        3) Handle concurrent events of driver execution and
        """

        try:
            # Configure environment variables for the child process. These
            # will *not* be set in the runtime_env, so they apply to the driver
            # only, not its tasks & actors.
            os.environ.update(self._get_driver_env_vars(resources_specified))

            self._logger.info(
                "Submitting job with RAY_ADDRESS = "
                f"{os.environ[ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE]}"
            )

            log_path = self._log_client.get_log_file_path(self._job_id)

            # Execute job's entrypoint in the subprocess
            child_process = self._exec_entrypoint(log_path)

            # Block until either of the following occurs:
            #   - Process executing job's entrypoint completes (exits, returning specific exit-code)
            #   - Stop API is invoked to interrupt job's entrypoint process
            polling_task = create_task(self._polling(child_process))
            stop_event_awaiting_task = create_task(self._stop_event.wait())

            finished, _ = await asyncio.wait(
                [polling_task, stop_event_awaiting_task],
                return_when=FIRST_COMPLETED,
            )

            if self._stop_event.is_set():
                # Cancel task polling the subprocess (unless already finished)
                if not polling_task.done():
                    polling_task.cancel()
                # Stop the subprocess running job's entrypoint
                await self.stop_process(child_process.pid)

                return JobExecutionResult(
                    driver_exit_code=-1,
                    stopped=True,
                )
            else:
                # Child process finished execution and no stop event is set
                # at the same time
                assert len(finished) == 1, "Should have only one coroutine done"

                [child_process_task] = finished

                return_code = child_process_task.result()

                logger.info(
                    f"Job {self._job_id} entrypoint command "
                    f"exited with code {return_code}"
                )

                message: Optional[str] = None

                if return_code != 0:
                    raw_log_snippet = self._log_client.get_last_n_log_lines(
                        self._job_id
                    )
                    if raw_log_snippet:
                        truncated_log_snippet = (
                            "Last available logs (truncated to 20,000 chars):\n"
                            + raw_log_snippet
                        )
                    else:
                        truncated_log_snippet = "No logs available."

                    message = f"Job entrypoint command failed with exit code {return_code}. {truncated_log_snippet}"

                return JobExecutionResult(
                    driver_exit_code=return_code,
                    message=message,
                )

        except Exception:
            exception_message = traceback.format_exc()
            self._logger.error(
                f"Got unexpected exception while executing driver for job {self._job_id} "
                f"command. {exception_message}"
            )

            return JobExecutionResult(
                driver_exit_code=-1,
                message=exception_message,
            )
        finally:
            # Job's supervisor actor lifespan is bound to that of the job's driver,
            # and therefore upon completing its execution JobSupervisor (along with JobRunner)
            # actors could be subsequently destroyed
            ray.actor.exit_actor()

    async def stop_process(self, child_pid: int):
        if sys.platform == "win32" and self._win32_job_object:
            win32job.TerminateJobObject(self._win32_job_object, -1)
        elif sys.platform != "win32":
            stop_signal = os.environ.get("RAY_JOB_STOP_SIGNAL", "SIGTERM")
            if stop_signal not in self.VALID_STOP_SIGNALS:
                self._logger.warning(
                    f"{stop_signal} not a valid stop signal. Terminating "
                    "job with SIGTERM."
                )
                stop_signal = "SIGTERM"

            job_process = psutil.Process(child_pid)
            proc_to_kill = [job_process] + job_process.children(recursive=True)

            # Send stop signal and wait for job to terminate gracefully,
            # otherwise SIGKILL job forcefully after timeout.
            self._kill_processes(proc_to_kill, getattr(signal, stop_signal))

            stop_job_wait_time = int(
                os.environ.get(
                    "RAY_JOB_STOP_WAIT_TIME_S",
                    self.DEFAULT_RAY_JOB_STOP_WAIT_TIME_S,
                )
            )

            try:
                poll_job_stop_task = create_task(self._poll_all(proc_to_kill))
                await asyncio.wait_for(poll_job_stop_task, stop_job_wait_time)

                self._logger.info(
                    f"Job {self._job_id} has been terminated gracefully "
                    f"with {stop_signal}."
                )

            except asyncio.TimeoutError:
                self._logger.warning(
                    f"Attempt to gracefully terminate job {self._job_id} "
                    f"through {stop_signal} has timed out after "
                    f"{stop_job_wait_time} seconds. Job is now being "
                    "force-killed with SIGKILL."
                )
                self._kill_processes(proc_to_kill, signal.SIGKILL)
