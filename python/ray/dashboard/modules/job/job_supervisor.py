import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
import traceback
from asyncio.tasks import FIRST_COMPLETED
from typing import Any, Dict, List, Optional, Tuple

import ray
import ray._private.ray_constants as ray_constants
from ray._common.filters import CoreContextFilter
from ray._common.formatters import JSONFormatter, TextFormatter
from ray._common.network_utils import build_address
from ray._common.utils import hex_to_binary
from ray._private.accelerators.npu import NOSET_ASCEND_RT_VISIBLE_DEVICES_ENV_VAR
from ray._private.accelerators.nvidia_gpu import NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR
from ray._private.utils import remove_ray_internal_flags_from_env
from ray._raylet import GcsClient, JobID
from ray.actor import ActorHandle
from ray.core.generated.common_pb2 import (
    ActorDeathCause,
    ActorDiedErrorContext,
    ErrorType,
    InfraCauseContext,
    NodeDeathInfo,
    TaskStatus,
)
from ray.core.generated.gcs_service_pb2 import FilterPredicate, GetTaskEventsRequest
from ray.dashboard.modules.job.common import (
    JOB_ID_METADATA_KEY,
    JOB_LOGS_PATH_TEMPLATE,
    JOB_NAME_METADATA_KEY,
    JobFailureStage,
    JobInfoStorageClient,
    context_dict_from_proto,
    make_failure_info,
)
from ray.dashboard.modules.job.job_log_storage_client import JobLogStorageClient
from ray.job_submission import JobErrorType, JobStatus

import psutil

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

# The error types that mean a job's failure was not its entrypoint's fault,
# ordered most to least specific so that the first match wins. This is an
# allowlist, not a denylist: an ErrorType that is not named here is left
# unattributed rather than guessed at. TASK_EXECUTION_EXCEPTION is deliberately
# absent -- it is how Ray records a genuine application error, and it is the
# discriminator the whole read turns on.
#
# WORKER_DIED ranks last because the GCS also writes it as a bulk backfill, both
# when a worker is reported dead and when a job ends, so on its own it does not
# distinguish a real worker death from the GCS's own bookkeeping.
_INFRA_ERROR_TYPES = (
    ErrorType.NODE_DIED,
    ErrorType.OUT_OF_MEMORY,
    ErrorType.LOCAL_RAYLET_DIED,
    ErrorType.OWNER_DIED,
    ErrorType.ACTOR_DIED,
    ErrorType.WORKER_DIED,
)
_INFRA_ERROR_TYPE_RANK = {
    error_type: rank for rank, error_type in enumerate(_INFRA_ERROR_TYPES)
}

# Actor death reasons that are infra failures, mapped onto the same ErrorType
# vocabulary as the task path so that evidence from either source ranks together.
# RAY_KILL, OUT_OF_SCOPE and REF_DELETED are intentional teardown, so they are
# absent.
_INFRA_ACTOR_DEATH_REASONS = {
    ActorDiedErrorContext.NODE_DIED: ErrorType.NODE_DIED,
    ActorDiedErrorContext.OWNER_DIED: ErrorType.OWNER_DIED,
    ActorDiedErrorContext.WORKER_DIED: ErrorType.WORKER_DIED,
}

# Resolved here, at import, rather than referenced from inside JobSupervisor.
#
# `ray.remote` rebinds the class as `_modify_class.<locals>.Class`, which is
# defined in a local scope, so cloudpickle serialises the class BY VALUE and
# walks the globals every method references. A protobuf enum symbol such as
# `TaskStatus` is an EnumTypeWrapper -- an object, not a class -- and it is not
# picklable:
#
#     TypeError: cannot pickle 'google._upb._message.EnumDescriptor' object
#
# The enum *values* are plain ints, so binding them once at module scope keeps
# the wrapper out of every method's global set. Message classes (InfraCauseContext,
# GetTaskEventsRequest, ...) pickle fine and are used directly.
_FILTER_PREDICATE_EQUAL = FilterPredicate.EQUAL
_TASK_STATUS_FINISHED = TaskStatus.FINISHED


def _infra_cause_from_actor_death(
    death_cause: ActorDeathCause,
) -> Optional[Tuple[int, InfraCauseContext]]:
    """Describe an ActorDeathCause as a ranked infra cause, or None if it is not one.

    Shared by both reads below, because the same ActorDeathCause reaches both: a
    task that failed because an actor died carries the actor's whole death cause
    on RayErrorInfo.actor_died_error. Classifying it in one place is what keeps a
    creation-task failure -- an exception raised in the user's own ``__init__``,
    which Ray also reports with error_type ACTOR_DIED -- from reading as an infra
    failure through the task events while the actor read excludes it.
    """
    context_case = death_cause.WhichOneof("context")
    node_death_info = None
    if context_case == "oom_context":
        # A separate branch of ActorDeathCause rather than a message on the
        # generic one, which is why an OOM kill can be reported as such instead
        # of as a substring of some error text.
        error_type = ErrorType.OUT_OF_MEMORY
        error_message = death_cause.oom_context.error_message
    elif context_case == "actor_died_error_context":
        died = death_cause.actor_died_error_context
        error_type = _INFRA_ACTOR_DEATH_REASONS.get(died.reason)
        if error_type is None:
            return None
        error_message = died.error_message
        if died.HasField("node_death_info"):
            node_death_info = died.node_death_info
            if node_death_info.reason == NodeDeathInfo.AUTOSCALER_DRAIN_PREEMPTED:
                # A drained spot node is a node death whatever the actor-level
                # reason says it is.
                error_type = ErrorType.NODE_DIED
    else:
        # creation_task_failure_context is the user's own exception, and
        # runtime_env_failed_context and actor_unschedulable_context are each
        # attributed at their own stage. worker_bootstrap_context reaches here too
        # and is deliberately not treated as infra: it means a worker never
        # finished registering, which is most often the job's own image or
        # py_executable, not the platform.
        return None

    context = InfraCauseContext(error_type=error_type, error_message=error_message)
    context.actor_death_cause.CopyFrom(death_cause)
    if node_death_info is not None:
        context.node_death_info.CopyFrom(node_death_info)
    return _INFRA_ERROR_TYPE_RANK[error_type], context


# Infra attribution lives at module scope, not on JobSupervisor.
#
# `ray.remote` rebinds the class as `_modify_class.<locals>.Class`, which is
# defined in a local scope, so cloudpickle serialises it BY VALUE and walks the
# globals every method references. Anything unpicklable reachable that way -- a
# protobuf enum wrapper, a Cython extension type -- breaks submission for every
# job, not just the ones this code runs for. A module-level function is pickled
# BY REFERENCE and its globals are never walked, so keeping the protobuf and
# _raylet symbols out here makes that whole class of failure impossible rather
# than fixed one symbol at a time.
# Bounds on when the GCS can be read for a job-keyed infra cause. Both come
# from GCS config, and together they are why this is a window rather than a
# single read when the driver exits:
#
#   lower bound -- task events are flushed from the owning worker every
#     `task_events_report_interval_ms` (1s), and a dead worker's tasks are
#     stamped only `gcs_mark_task_failed_on_worker_dead_delay_ms` (1s) after
#     the worker is reported dead, so a read at exit sees nothing;
#   upper bound -- `gcs_mark_task_failed_on_job_done_delay_ms` (15s) after
#     the driver exits, the GCS overwrites every still non-terminal task of
#     the job with WORKER_DIED and "Job finishes ...", which destroys the
#     real cause.
#
# Staying well inside the upper bound matters more than covering every case:
# a read that lands after the overwrite returns a confident wrong answer, and
# no attribution is better than a wrong one. If either config value changes,
# these have to change with it.
_INFRA_SETTLE_S = 1.5
_INFRA_RETRY_INTERVAL_S = 2
_INFRA_DEADLINE_S = 4
_INFRA_RPC_TIMEOUT_S = 2
# A witness is enough to attribute the job, so the task read is capped. The
# GCS returns the most recent events first, which is where the failure that
# ended the driver is.
_INFRA_TASK_EVENT_LIMIT = 1000
_INFRA_SAMPLE_TASK_IDS = 3


async def _resolve_driver_ray_job_id(gcs_client, submission_id) -> Optional[str]:
    """Return the hex Ray job id of this submission's driver, if it has one.

    The job record is keyed by submission id while the actor and task tables
    are keyed by Ray job id, so this is the bridge between the two.

    None when the driver never registered a Ray job, which is the case for an
    entrypoint that fails before calling ``ray.init()``. That is also what
    keeps the most common failure -- a script that raises on its own -- off
    the settle path below.
    """
    job_infos = await gcs_client.async_get_all_job_info(
        job_or_submission_id=submission_id,
        # Neither field is read here and both cost the GCS extra work per
        # job. The submission id filter is itself a scan over all jobs
        # because submission id is not indexed, so this call is kept as
        # cheap as it can be.
        skip_submission_job_info_field=True,
        skip_is_running_tasks_field=True,
        timeout=_INFRA_RPC_TIMEOUT_S,
    )
    ray_job_id = None
    # Sorted with the last match winning, following the convention elsewhere
    # that the highest job id is a submission's most recent driver.
    for job_table_entry in sorted(
        job_infos.values(), key=lambda entry: entry.job_id.hex()
    ):
        metadata = dict(job_table_entry.config.metadata)
        if metadata.get(JOB_ID_METADATA_KEY) == submission_id:
            ray_job_id = job_table_entry.job_id.hex()
    return ray_job_id


async def _infra_cause_from_dead_actors(
    gcs_client, ray_job_id: str
) -> Optional[Tuple[int, InfraCauseContext]]:
    """Look for an infra cause in this job's dead actors' death causes."""
    actors = await gcs_client.async_get_all_actor_info(
        job_id=JobID(hex_to_binary(ray_job_id)),
        # Filtered in the GCS: only a dead actor carries a death cause.
        actor_state_name="DEAD",
        timeout=_INFRA_RPC_TIMEOUT_S,
    )

    best = None
    for actor in actors.values():
        candidate = _infra_cause_from_actor_death(actor.death_cause)
        if candidate is None:
            continue
        if best is None or candidate[0] < best[0]:
            best = candidate
    return best


async def _infra_cause_from_task_events(
    ray_job_id: str, task_info_stub
) -> Optional[Tuple[int, InfraCauseContext]]:
    """Look for an infra cause in this job's task events."""
    request = GetTaskEventsRequest(
        limit=_INFRA_TASK_EVENT_LIMIT,
        filters=GetTaskEventsRequest.Filters(
            job_filters=[
                GetTaskEventsRequest.Filters.JobIdFilter(
                    predicate=_FILTER_PREDICATE_EQUAL,
                    job_id=JobID(hex_to_binary(ray_job_id)).binary(),
                )
            ],
            # The driver's own records carry profiling events only.
            exclude_driver=True,
        ),
    )
    reply = await task_info_stub.GetTaskEvents(request, timeout=_INFRA_RPC_TIMEOUT_S)

    # An attempt that failed and was then retried successfully is not a
    # failure of this job. The GCS keeps the failed attempt along with its
    # error_info even when the task was going to be retried, so without this
    # infra causes are over-reported.
    retried_successfully = {
        events.task_id
        for events in reply.events_by_task
        if events.HasField("state_updates")
        and _TASK_STATUS_FINISHED in events.state_updates.state_ts_ns
    }

    best = None
    sample_task_ids = []
    for events in reply.events_by_task:
        if events.task_id in retried_successfully:
            continue
        if not events.HasField("state_updates"):
            continue
        # Gated on presence: an unset error_info reads back as the default
        # message, whose error_type is the zero value WORKER_DIED, so
        # ungated every task without an error looks like a worker death.
        if not events.state_updates.HasField("error_info"):
            continue
        error_info = events.state_updates.error_info
        if error_info.HasField("actor_died_error"):
            # The task failed because an actor did, and the actor's own
            # death cause came along whole. Classify on that rather than on
            # error_type.
            candidate = _infra_cause_from_actor_death(error_info.actor_died_error)
            if candidate is None:
                continue
            rank, context = candidate
        else:
            rank = _INFRA_ERROR_TYPE_RANK.get(error_info.error_type)
            if rank is None:
                continue
            context = InfraCauseContext(
                error_type=error_info.error_type,
                error_message=error_info.error_message,
            )

        if best is not None and rank > best[0]:
            continue
        if best is None or rank < best[0]:
            # A more specific cause than anything seen so far, so the sample
            # belongs to it rather than to the one it replaces.
            best = (rank, context)
            sample_task_ids = []
        # Deduplicated: a retried task appears once per attempt, each attempt a
        # separate events_by_task entry with the same task_id. Without this, a
        # single task that failed its default three attempts fills the whole
        # sample with one id repeated, which reads as three independent
        # witnesses when there is only one.
        task_id_hex = events.task_id.hex()
        if (
            len(sample_task_ids) < _INFRA_SAMPLE_TASK_IDS
            and task_id_hex not in sample_task_ids
        ):
            sample_task_ids.append(task_id_hex)

    if best is None:
        return None
    best[1].sample_task_ids.extend(sample_task_ids)
    return best


async def _poll_for_infra_cause(
    gcs_client, gcs_address, submission_id, driver_exit_observed_at: float
) -> Optional[Dict[str, Any]]:
    """Read this job's actor and task records for an infra cause.

    Returns the highest-ranked cause found as an InfraCauseContext dict, or
    None. Only records Ray itself keyed to this job are read: no log, stderr
    or exception text is inspected anywhere in here.
    """
    ray_job_id = await _resolve_driver_ray_job_id(gcs_client, submission_id)
    if ray_job_id is None:
        return None

    # Imported here rather than at module scope because a JobSupervisor is
    # started for every job, including the ones that succeed, and this
    # pulls grpc in.
    from ray._private.grpc_utils import init_grpc_channel
    from ray.core.generated import gcs_service_pb2_grpc

    channel = init_grpc_channel(
        gcs_address,
        options=ray_constants.GLOBAL_GRPC_OPTIONS,
        asynchronous=True,
    )
    try:
        task_info_stub = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(channel)
        await asyncio.sleep(_INFRA_SETTLE_S)
        while True:
            found = [
                candidate
                for candidate in (
                    await _infra_cause_from_dead_actors(gcs_client, ray_job_id),
                    await _infra_cause_from_task_events(ray_job_id, task_info_stub),
                )
                if candidate is not None
            ]
            if found:
                _, context = min(found, key=lambda candidate: candidate[0])
                context.ray_job_id = JobID(hex_to_binary(ray_job_id)).binary()
                # error_type has no presence and its zero value is
                # WORKER_DIED, so a WORKER_DIED cause is written without it;
                # that parses back to the same value.
                return context_dict_from_proto(context)
            # Counting the sleep, not just the time already spent: the point
            # is to never read outside the window, so give up rather than
            # start a round that would finish after it closes.
            if (
                time.monotonic() + _INFRA_RETRY_INTERVAL_S - driver_exit_observed_at
                >= _INFRA_DEADLINE_S
            ):
                return None
            await asyncio.sleep(_INFRA_RETRY_INTERVAL_S)
    finally:
        await channel.close()


async def attribute_driver_failure_to_infra(
    gcs_client, gcs_address, submission_id, driver_exit_observed_at: float, logger
) -> Optional[Dict[str, Any]]:
    """Best-effort infra attribution for a driver that exited non-zero.

    Falls back to reporting nothing on every failure path -- no Ray job id,
    nothing recorded for the job, a slow or unavailable GCS, an unexpected
    error -- because a wrong attribution is worse than none, and because a
    hang in here would turn a cleanly failed job into a stuck one.
    """
    try:
        return await asyncio.wait_for(
            _poll_for_infra_cause(
                gcs_client, gcs_address, submission_id, driver_exit_observed_at
            ),
            # The inner loop bounds itself too; this is the backstop for a
            # single call that never returns.
            timeout=(_INFRA_SETTLE_S + _INFRA_DEADLINE_S + _INFRA_RETRY_INTERVAL_S),
        )
    except Exception:
        logger.info(
            f"Could not read an infra cause for job {submission_id}. "
            "Reporting the driver's exit alone.",
            exc_info=True,
        )
        return None


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
        cluster_id_hex: str,
        logs_dir: Optional[str] = None,
    ):
        self._job_id = job_id
        self._gcs_address = gcs_address
        # Retained: a failed job is attributed by reading Ray's own actor and
        # task records for it. Deliberately the GCS client and not
        # ray.util.state, whose list_actors/list_tasks go over HTTP to the
        # dashboard API server -- that would make attribution depend on a
        # component that is itself implicated in some of the failures being
        # attributed.
        self._gcs_client = GcsClient(address=gcs_address, cluster_id=cluster_id_hex)
        self._job_info_client = JobInfoStorageClient(self._gcs_client, logs_dir)
        self._log_client = JobLogStorageClient()
        self._entrypoint = entrypoint

        # Default metadata if not passed by the user.
        self._metadata = {JOB_ID_METADATA_KEY: job_id, JOB_NAME_METADATA_KEY: job_id}
        self._metadata.update(user_metadata)

        # Event used to signal that a job should be stopped.
        # Set in the `stop_job` method.
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
        self._logger.addFilter(CoreContextFilter())
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(supervisor_log_file_name)
        formatter = TextFormatter()
        if ray_constants.env_bool(ray_constants.RAY_BACKEND_LOG_JSON_ENV_VAR, False):
            formatter = JSONFormatter()
        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        self._logger.addHandler(stream_handler)
        self._logger.addHandler(file_handler)
        self._logger.propagate = False

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
        env_vars.pop(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR)
        env_vars.pop(NOSET_ASCEND_RT_VISIBLE_DEVICES_ENV_VAR)
        env_vars.pop(ray_constants.RAY_WORKER_NICENESS)
        curr_runtime_env["env_vars"] = env_vars
        return curr_runtime_env

    def ping(self):
        """Used to check the health of the actor."""
        pass

    def _exec_entrypoint(self, env: dict, logs_path: str) -> subprocess.Popen:
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
            env: Environment variables passed through to the driver subprocess.
            logs_path: File path on head node's local disk to store driver
                command's stdout & stderr.
        Returns:
            child_process: Child process that runs the driver command. Can be
                terminated or killed upon user calling stop().
        """
        # Open in append mode to avoid overwriting runtime_env setup logs for the
        # supervisor actor, which are also written to the same file.
        with open(logs_path, "a") as logs_file:
            logs_file.write(
                f"Running entrypoint for job {self._job_id}: {self._entrypoint}\n"
            )
            child_process = subprocess.Popen(
                self._entrypoint,
                shell=True,
                start_new_session=True,
                stdout=logs_file,
                stderr=subprocess.STDOUT,
                env=env,
                # Ray intentionally blocks SIGINT in all processes, so if the user wants
                # to stop job through SIGINT, we need to unblock it in the child process
                preexec_fn=(
                    (
                        lambda: signal.pthread_sigmask(
                            signal.SIG_UNBLOCK, {signal.SIGINT}
                        )
                    )
                    if sys.platform != "win32"
                    and os.environ.get("RAY_JOB_STOP_SIGNAL") == "SIGINT"
                    else None
                ),
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
                #
                # start_new_session=True detaches this watcher into its own
                # session/process group. Otherwise it would inherit the supervisor
                # actor's process group and the raylet's per-worker process-group
                # cleanup (killpg on worker exit) would kill this watcher before it
                # can reap the driver, potentially leaking the driver subprocess.
                subprocess.Popen(
                    f"while kill -s 0 {parent_pid}; do sleep 1; done; kill -9 -{child_pgid}",  # noqa: E501
                    shell=True,
                    start_new_session=True,
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
            "auto", ray._private.worker._global_node._ray_params.temp_dir
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

    async def run(
        self,
        # Signal actor used in testing to capture PENDING -> RUNNING cases
        _start_signal_actor: Optional[ActorHandle] = None,
        resources_specified: bool = False,
    ):
        """
        Stop and start both happen asynchronously, coordinated by asyncio event
        and coroutine, respectively.

        1) Sets job status as running
        2) Pass runtime env and metadata to subprocess as serialized env
            variables.
        3) Handle concurrent events of driver execution and
        """
        curr_info = await self._job_info_client.get_info(self._job_id)
        if curr_info is None:
            raise RuntimeError(f"Status could not be retrieved for job {self._job_id}.")
        curr_status = curr_info.status
        curr_message = curr_info.message
        if curr_status == JobStatus.RUNNING:
            raise RuntimeError(
                f"Job {self._job_id} is already in RUNNING state. "
                f"JobSupervisor.run() should only be called once. "
            )
        if curr_status != JobStatus.PENDING:
            raise RuntimeError(
                f"Job {self._job_id} is not in PENDING state. "
                f"Current status is {curr_status} with message {curr_message}."
            )

        if _start_signal_actor:
            # Block in PENDING state until start signal received.
            await _start_signal_actor.wait.remote()

        node = ray._private.worker.global_worker.node
        driver_agent_http_address = f"http://{build_address(node.node_ip_address, node.dashboard_agent_listen_port)}"
        driver_node_id = ray.get_runtime_context().get_node_id()

        await self._job_info_client.put_status(
            self._job_id,
            JobStatus.RUNNING,
            jobinfo_replace_kwargs={
                "driver_agent_http_address": driver_agent_http_address,
                "driver_node_id": driver_node_id,
            },
        )

        try:
            # Configure environment variables for the child process.
            env = os.environ.copy()
            # Remove internal Ray flags. They present because JobSuperVisor itself is
            # a Ray worker process but we don't want to pass them to the driver.
            remove_ray_internal_flags_from_env(env)
            # These will *not* be set in the runtime_env, so they apply to the driver
            # only, not its tasks & actors.
            env.update(self._get_driver_env_vars(resources_specified))

            self._logger.info(
                "Submitting job with RAY_ADDRESS = "
                f"{env[ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE]}"
            )
            log_path = self._log_client.get_log_file_path(self._job_id)
            child_process = self._exec_entrypoint(env, log_path)
            child_pid = child_process.pid

            polling_task = create_task(self._polling(child_process))
            finished, _ = await asyncio.wait(
                [polling_task, create_task(self._stop_event.wait())],
                return_when=FIRST_COMPLETED,
            )

            if self._stop_event.is_set():
                polling_task.cancel()
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
                    try:
                        stop_job_wait_time = int(
                            os.environ.get(
                                "RAY_JOB_STOP_WAIT_TIME_S",
                                self.DEFAULT_RAY_JOB_STOP_WAIT_TIME_S,
                            )
                        )
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

                await self._job_info_client.put_status(self._job_id, JobStatus.STOPPED)
            else:
                # Child process finished execution and no stop event is set
                # at the same time
                assert len(finished) == 1, "Should have only one coroutine done"
                [child_process_task] = finished
                return_code = child_process_task.result()
                # Taken here, not in the helper below: this is when the driver
                # exited, which is also when the GCS starts the clock on
                # overwriting the records the helper reads.
                driver_exit_observed_at = time.monotonic()
                self._logger.info(
                    f"Job {self._job_id} entrypoint command "
                    f"exited with code {return_code}"
                )
                if return_code == 0:
                    await self._job_info_client.put_status(
                        self._job_id,
                        JobStatus.SUCCEEDED,
                        driver_exit_code=return_code,
                    )
                else:
                    log_excerpt_ref = None
                    log_tail = await self._log_client.get_last_n_log_lines(self._job_id)
                    if log_tail is not None and log_tail != "":
                        message = (
                            "Job entrypoint command "
                            f"failed with exit code {return_code}, "
                            "last available logs (truncated to 20,000 chars):\n"
                            + log_tail
                        )
                        # Where those logs are, rather than the logs themselves.
                        # The excerpt is the driver's own output, and
                        # DriverRunContext is served by the state and export
                        # APIs; the log file is already reachable through the
                        # job logs API, which is where it should be read from.
                        log_excerpt_ref = (
                            f"{driver_node_id}:"
                            + JOB_LOGS_PATH_TEMPLATE.format(submission_id=self._job_id)
                        )
                    else:
                        message = (
                            "Job entrypoint command "
                            f"failed with exit code {return_code}. No logs available."
                        )
                    # A non-zero exit is not on its own evidence that the
                    # entrypoint is what failed. Ask Ray what it recorded for
                    # this job before saying so.
                    infra_cause = await attribute_driver_failure_to_infra(
                        self._gcs_client,
                        self._gcs_address,
                        self._job_id,
                        driver_exit_observed_at,
                        self._logger,
                    )
                    error_type = JobErrorType.JOB_ENTRYPOINT_COMMAND_ERROR
                    if infra_cause is not None:
                        error_type = JobErrorType.JOB_DRIVER_INFRA_FAILURE
                    await self._job_info_client.put_status(
                        self._job_id,
                        JobStatus.FAILED,
                        # Unchanged, including the log tail: this string is what
                        # existing consumers read.
                        message=message,
                        driver_exit_code=return_code,
                        error_type=error_type,
                        failure_info=make_failure_info(
                            JobFailureStage.DRIVER_RUN,
                            driver_exit_code=return_code,
                            context_key="driver_run",
                            context={
                                "error_message": (
                                    "Job entrypoint command failed with exit "
                                    f"code {return_code}."
                                )
                            },
                            log_excerpt_ref=log_excerpt_ref,
                            # A sibling of driver_run rather than a replacement:
                            # the driver did exit non-zero, it is just not the
                            # entrypoint that failed.
                            infra_cause=infra_cause,
                        ),
                    )
        except Exception as e:
            self._logger.error(
                "Got unexpected exception while trying to execute driver "
                f"command. {traceback.format_exc()}"
            )
            try:
                await self._job_info_client.put_status(
                    self._job_id,
                    JobStatus.FAILED,
                    message=traceback.format_exc(),
                    error_type=JobErrorType.JOB_ENTRYPOINT_COMMAND_START_ERROR,
                    # The entrypoint never began executing, which is a different
                    # fault from running and exiting non-zero. driver_exit_code
                    # stays unset here, as it is today.
                    failure_info=make_failure_info(
                        JobFailureStage.DRIVER_RUN,
                        context_key="driver_run",
                        context={
                            "error_message": traceback.format_exc(),
                            "exception_class": type(e).__name__,
                            "failed_to_start": True,
                        },
                    ),
                )
            except Exception:
                self._logger.error(
                    "Failed to update job status to FAILED. "
                    f"Exception: {traceback.format_exc()}"
                )
        finally:
            # clean up actor after tasks are finished
            ray.actor.exit_actor()

    def stop(self):
        """Set step_event and let run() handle the rest in its asyncio.wait()."""
        self._stop_event.set()
