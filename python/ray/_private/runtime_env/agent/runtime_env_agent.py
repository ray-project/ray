import asyncio
import logging
import os
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, List, Set, Tuple
from ray._private.ray_constants import (
    DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS,
)

import ray._private.runtime_env.agent.runtime_env_consts as runtime_env_consts
from ray._private.ray_logging import setup_component_logger
from ray._private.runtime_env.conda import CondaPlugin
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.default_impl import get_image_uri_plugin_cls
from ray._private.runtime_env.java_jars import JavaJarsPlugin
from ray._private.runtime_env.image_uri import ContainerPlugin
from ray._private.runtime_env.pip import PipPlugin
from ray._private.runtime_env.uv import UvPlugin
from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.plugin import (
    RuntimeEnvPlugin,
    create_for_plugin_if_needed,
)
from ray._private.utils import get_or_create_event_loop
from ray._private.runtime_env.plugin import RuntimeEnvPluginManager
from ray._private.runtime_env.py_modules import PyModulesPlugin
from ray._private.runtime_env.working_dir import WorkingDirPlugin
from ray._private.runtime_env.nsight import NsightPlugin
from ray._private.runtime_env.py_executable import PyExecutablePlugin
from ray._private.runtime_env.mpi import MPIPlugin
from ray.core.generated import (
    runtime_env_agent_pb2,
    agent_manager_pb2,
)
from ray.core.generated.runtime_env_common_pb2 import (
    RuntimeEnvState as ProtoRuntimeEnvState,
)
from ray.runtime_env import RuntimeEnv, RuntimeEnvConfig

default_logger = logging.getLogger(__name__)

# TODO(edoakes): this is used for unit tests. We should replace it with a
# better pluggability mechanism once available.
SLEEP_FOR_TESTING_S = os.environ.get("RAY_RUNTIME_ENV_SLEEP_FOR_TESTING_S")


@dataclass
class CreatedEnvResult:
    # Whether or not the env was installed correctly.
    success: bool
    # If success is True, will be a serialized RuntimeEnvContext
    # If success is False, will be an error message.
    result: str
    # The time to create a runtime env in ms.
    creation_time_ms: int


# e.g., "working_dir"
UriType = str


class ReferenceTable:
    """
    The URI reference table which is used for GC.
    When the reference count is decreased to zero,
    the URI should be removed from this table and
    added to cache if needed.
    """

    def __init__(
        self,
        uris_parser: Callable[[RuntimeEnv], Tuple[str, UriType]],
        unused_uris_callback: Callable[[List[Tuple[str, UriType]]], None],
        unused_runtime_env_callback: Callable[[str], None],
    ):
        # Runtime Environment reference table. The key is serialized runtime env and
        # the value is reference count.
        self._runtime_env_reference: Dict[str, int] = defaultdict(int)
        # URI reference table. The key is URI parsed from runtime env and the value
        # is reference count.
        self._uri_reference: Dict[str, int] = defaultdict(int)
        self._uris_parser = uris_parser
        self._unused_uris_callback = unused_uris_callback
        self._unused_runtime_env_callback = unused_runtime_env_callback
        # send the `DeleteRuntimeEnvIfPossible` RPC when the client exits. The URI won't
        # be leaked now because the reference count will be reset to zero when the job
        # finished.
        self._reference_exclude_sources: Set[str] = {
            "client_server",
        }

    def _increase_reference_for_uris(self, uris):
        default_logger.debug(f"Increase reference for uris {uris}.")
        for uri, _ in uris:
            self._uri_reference[uri] += 1

    def _decrease_reference_for_uris(self, uris):
        default_logger.debug(f"Decrease reference for uris {uris}.")
        unused_uris = list()
        for uri, uri_type in uris:
            if self._uri_reference[uri] > 0:
                self._uri_reference[uri] -= 1
                if self._uri_reference[uri] == 0:
                    unused_uris.append((uri, uri_type))
                    del self._uri_reference[uri]
            else:
                default_logger.warning(f"URI {uri} does not exist.")
        if unused_uris:
            default_logger.info(f"Unused uris {unused_uris}.")
            self._unused_uris_callback(unused_uris)
        return unused_uris

    def _increase_reference_for_runtime_env(self, serialized_env: str):
        default_logger.debug(f"Increase reference for runtime env {serialized_env}.")
        self._runtime_env_reference[serialized_env] += 1

    def _decrease_reference_for_runtime_env(self, serialized_env: str):
        """Decrease reference count for the given [serialized_env]. Throw exception if we cannot decrement reference."""
        default_logger.debug(f"Decrease reference for runtime env {serialized_env}.")
        unused = False
        if self._runtime_env_reference[serialized_env] > 0:
            self._runtime_env_reference[serialized_env] -= 1
            if self._runtime_env_reference[serialized_env] == 0:
                unused = True
                del self._runtime_env_reference[serialized_env]
        else:
            default_logger.warning(f"Runtime env {serialized_env} does not exist.")
            raise ValueError(
                f"{serialized_env} cannot decrement reference since the reference count is 0"
            )
        if unused:
            default_logger.info(f"Unused runtime env {serialized_env}.")
            self._unused_runtime_env_callback(serialized_env)

    def increase_reference(
        self, runtime_env: RuntimeEnv, serialized_env: str, source_process: str
    ) -> None:
        if source_process in self._reference_exclude_sources:
            return
        self._increase_reference_for_runtime_env(serialized_env)
        uris = self._uris_parser(runtime_env)
        self._increase_reference_for_uris(uris)

    def decrease_reference(
        self, runtime_env: RuntimeEnv, serialized_env: str, source_process: str
    ) -> None:
        """Decrease reference count for runtime env and uri. Throw exception if decrement reference count fails."""
        if source_process in self._reference_exclude_sources:
            return
        self._decrease_reference_for_runtime_env(serialized_env)
        uris = self._uris_parser(runtime_env)
        self._decrease_reference_for_uris(uris)

    @property
    def runtime_env_refs(self) -> Dict[str, int]:
        """Return the runtime_env -> ref count mapping.

        Returns:
            The mapping of serialized runtime env -> ref count.
        """
        return self._runtime_env_reference


class RuntimeEnvAgent:
    """An RPC server to create and delete runtime envs.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config.
    """

    LOG_FILENAME = "runtime_env_agent.log"

    def __init__(
        self,
        runtime_env_dir,
        logging_params,
        gcs_address: str,
        cluster_id_hex: str,
        temp_dir,
        address,
        runtime_env_agent_port,
    ):
        super().__init__()

        self._logger = default_logger
        self._logging_params = logging_params
        self._logging_params.update(filename=self.LOG_FILENAME)
        self._logger = setup_component_logger(
            logger_name=default_logger.name, **self._logging_params
        )
        # Don't propagate logs to the root logger, because these logs
        # might contain sensitive information. Instead, these logs should
        # be confined to the runtime env agent log file `self.LOG_FILENAME`.
        self._logger.propagate = False

        self._logger.info("Starting runtime env agent at pid %s", os.getpid())
        self._logger.info(f"Parent raylet pid is {os.environ.get('RAY_RAYLET_PID')}")

        self._runtime_env_dir = runtime_env_dir
        self._per_job_logger_cache = dict()
        # Cache the results of creating envs to avoid repeatedly calling into
        # conda and other slow calls.
        self._env_cache: Dict[str, CreatedEnvResult] = dict()
        # Maps a serialized runtime env to a lock that is used
        # to prevent multiple concurrent installs of the same env.
        self._env_locks: Dict[str, asyncio.Lock] = dict()
        self._gcs_aio_client = GcsAioClient(
            address=gcs_address, cluster_id=cluster_id_hex
        )

        self._pip_plugin = PipPlugin(self._runtime_env_dir)
        self._uv_plugin = UvPlugin(self._runtime_env_dir)
        self._conda_plugin = CondaPlugin(self._runtime_env_dir)
        self._py_modules_plugin = PyModulesPlugin(
            self._runtime_env_dir, self._gcs_aio_client
        )
        self._py_executable_plugin = PyExecutablePlugin()
        self._java_jars_plugin = JavaJarsPlugin(
            self._runtime_env_dir, self._gcs_aio_client
        )
        self._working_dir_plugin = WorkingDirPlugin(
            self._runtime_env_dir, self._gcs_aio_client
        )
        self._container_plugin = ContainerPlugin(temp_dir)
        # TODO(jonathan-anyscale): change the plugin to ProfilerPlugin
        # and unify with nsight and other profilers.
        self._nsight_plugin = NsightPlugin(self._runtime_env_dir)
        self._mpi_plugin = MPIPlugin()
        self._image_uri_plugin = get_image_uri_plugin_cls()(temp_dir)

        # TODO(architkulkarni): "base plugins" and third-party plugins should all go
        # through the same code path.  We should never need to refer to
        # self._xxx_plugin, we should just iterate through self._plugins.
        self._base_plugins: List[RuntimeEnvPlugin] = [
            self._working_dir_plugin,
            self._uv_plugin,
            self._pip_plugin,
            self._conda_plugin,
            self._py_modules_plugin,
            self._py_executable_plugin,
            self._java_jars_plugin,
            self._container_plugin,
            self._nsight_plugin,
            self._mpi_plugin,
            self._image_uri_plugin,
        ]
        self._plugin_manager = RuntimeEnvPluginManager()
        for plugin in self._base_plugins:
            self._plugin_manager.add_plugin(plugin)

        self._reference_table = ReferenceTable(
            self.uris_parser,
            self.unused_uris_processor,
            self.unused_runtime_env_processor,
        )

        self._logger.info(
            "Listening to address %s, port %d", address, runtime_env_agent_port
        )

    def uris_parser(self, runtime_env: RuntimeEnv):
        result = list()
        for name, plugin_setup_context in self._plugin_manager.plugins.items():
            plugin = plugin_setup_context.class_instance
            uris = plugin.get_uris(runtime_env)
            for uri in uris:
                result.append((uri, UriType(name)))
        return result

    def unused_uris_processor(self, unused_uris: List[Tuple[str, UriType]]) -> None:
        for uri, uri_type in unused_uris:
            self._plugin_manager.plugins[str(uri_type)].uri_cache.mark_unused(uri)

    def unused_runtime_env_processor(self, unused_runtime_env: str) -> None:
        def delete_runtime_env():
            del self._env_cache[unused_runtime_env]
            self._logger.info(
                "Runtime env %s removed from env-level cache.", unused_runtime_env
            )

        if unused_runtime_env in self._env_cache:
            if not self._env_cache[unused_runtime_env].success:
                loop = get_or_create_event_loop()
                # Cache the bad runtime env result by ttl seconds.
                loop.call_later(
                    runtime_env_consts.BAD_RUNTIME_ENV_CACHE_TTL_SECONDS,
                    delete_runtime_env,
                )
            else:
                delete_runtime_env()

    def get_or_create_logger(self, job_id: bytes, log_files: List[str]):
        job_id = job_id.decode()
        if job_id not in self._per_job_logger_cache:
            params = self._logging_params.copy()
            params["filename"] = [f"runtime_env_setup-{job_id}.log", *log_files]
            params["logger_name"] = f"runtime_env_{job_id}"
            params["propagate"] = False
            per_job_logger = setup_component_logger(**params)
            self._per_job_logger_cache[job_id] = per_job_logger
        return self._per_job_logger_cache[job_id]

    async def GetOrCreateRuntimeEnv(self, request):
        self._logger.debug(
            f"Got request from {request.source_process} to increase "
            "reference for runtime env: "
            f"{request.serialized_runtime_env}."
        )

        async def _setup_runtime_env(
            runtime_env: RuntimeEnv,
            runtime_env_config: RuntimeEnvConfig,
        ):
            log_files = runtime_env_config.get("log_files", [])
            # Use a separate logger for each job.
            per_job_logger = self.get_or_create_logger(request.job_id, log_files)
            context = RuntimeEnvContext(env_vars=runtime_env.env_vars())

            # Warn about unrecognized fields in the runtime env.
            for name, _ in runtime_env.plugins():
                if name not in self._plugin_manager.plugins:
                    per_job_logger.warning(
                        f"runtime_env field {name} is not recognized by "
                        "Ray and will be ignored.  In the future, unrecognized "
                        "fields in the runtime_env will raise an exception."
                    )

            # Creates each runtime env URI by their priority. `working_dir` is special
            # because it needs to be created before other plugins. All other plugins are
            # created in the priority order (smaller priority value -> earlier to
            # create), with a special environment variable being set to the working dir.
            # ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}

            # First create working dir...
            working_dir_ctx = self._plugin_manager.plugins[WorkingDirPlugin.name]
            await create_for_plugin_if_needed(
                runtime_env,
                working_dir_ctx.class_instance,
                working_dir_ctx.uri_cache,
                context,
                per_job_logger,
            )

            # Then within the working dir, create the other plugins.
            working_dir_uri_or_none = runtime_env.working_dir_uri()
            with self._working_dir_plugin.with_working_dir_env(working_dir_uri_or_none):
                """Run setup for each plugin unless it has already been cached."""
                for (
                    plugin_setup_context
                ) in self._plugin_manager.sorted_plugin_setup_contexts():
                    plugin = plugin_setup_context.class_instance
                    if plugin.name != WorkingDirPlugin.name:
                        uri_cache = plugin_setup_context.uri_cache
                        await create_for_plugin_if_needed(
                            runtime_env, plugin, uri_cache, context, per_job_logger
                        )
            return context

        async def _create_runtime_env_with_retry(
            runtime_env,
            setup_timeout_seconds,
            runtime_env_config: RuntimeEnvConfig,
        ) -> Tuple[bool, str, str]:
            """
            Create runtime env with retry times. This function won't raise exceptions.

            Args:
                runtime_env: The instance of RuntimeEnv class.
                setup_timeout_seconds: The timeout of runtime environment creation for
                each attempt.

            Returns:
                a tuple which contains result (bool), runtime env context (str), error
                message(str).

            """
            self._logger.info(
                f"Creating runtime env: {serialized_env} with timeout "
                f"{setup_timeout_seconds} seconds."
            )
            serialized_context = None
            error_message = None
            for _ in range(runtime_env_consts.RUNTIME_ENV_RETRY_TIMES):
                try:
                    runtime_env_setup_task = _setup_runtime_env(
                        runtime_env, runtime_env_config
                    )
                    runtime_env_context = await asyncio.wait_for(
                        runtime_env_setup_task, timeout=setup_timeout_seconds
                    )
                    serialized_context = runtime_env_context.serialize()
                    error_message = None
                    break
                except Exception as e:
                    err_msg = f"Failed to create runtime env {serialized_env}."
                    self._logger.exception(err_msg)
                    error_message = "".join(
                        traceback.format_exception(type(e), e, e.__traceback__)
                    )
                    if isinstance(e, asyncio.TimeoutError):
                        hint = (
                            f"Failed to install runtime_env within the "
                            f"timeout of {setup_timeout_seconds} seconds. Consider "
                            "increasing the timeout in the runtime_env config. "
                            "For example: \n"
                            '    runtime_env={"config": {"setup_timeout_seconds":'
                            " 1800}, ...}\n"
                            "If not provided, the default timeout is "
                            f"{DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS} seconds. "
                        )
                        error_message = hint + error_message
                    await asyncio.sleep(
                        runtime_env_consts.RUNTIME_ENV_RETRY_INTERVAL_MS / 1000
                    )
            if error_message:
                self._logger.error(
                    "Runtime env creation failed for %d times, "
                    "don't retry any more.",
                    runtime_env_consts.RUNTIME_ENV_RETRY_TIMES,
                )
                return False, None, error_message
            else:
                self._logger.info(
                    "Successfully created runtime env: %s, the context: %s",
                    serialized_env,
                    serialized_context,
                )
                return True, serialized_context, None

        try:
            serialized_env = request.serialized_runtime_env
            runtime_env = RuntimeEnv.deserialize(serialized_env)
        except Exception as e:
            self._logger.exception(
                "[Increase] Failed to parse runtime env: " f"{serialized_env}"
            )
            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message="".join(
                    traceback.format_exception(type(e), e, e.__traceback__)
                ),
            )

        # Increase reference
        self._reference_table.increase_reference(
            runtime_env, serialized_env, request.source_process
        )

        if serialized_env not in self._env_locks:
            # async lock to prevent the same env being concurrently installed
            self._env_locks[serialized_env] = asyncio.Lock()

        async with self._env_locks[serialized_env]:
            if serialized_env in self._env_cache:
                serialized_context = self._env_cache[serialized_env]
                result = self._env_cache[serialized_env]
                if result.success:
                    context = result.result
                    self._logger.info(
                        "Runtime env already created "
                        f"successfully. Env: {serialized_env}, "
                        f"context: {context}"
                    )
                    return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                        status=agent_manager_pb2.AGENT_RPC_STATUS_OK,
                        serialized_runtime_env_context=context,
                    )
                else:
                    error_message = result.result
                    self._logger.info(
                        "Runtime env already failed. "
                        f"Env: {serialized_env}, "
                        f"err: {error_message}"
                    )
                    # Recover the reference.
                    self._reference_table.decrease_reference(
                        runtime_env, serialized_env, request.source_process
                    )
                    return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                        status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                        error_message=error_message,
                    )

            if SLEEP_FOR_TESTING_S:
                self._logger.info(f"Sleeping for {SLEEP_FOR_TESTING_S}s.")
                time.sleep(int(SLEEP_FOR_TESTING_S))

            runtime_env_config = RuntimeEnvConfig.from_proto(request.runtime_env_config)

            # accroding to the document of `asyncio.wait_for`,
            # None means disable timeout logic
            setup_timeout_seconds = (
                None
                if runtime_env_config["setup_timeout_seconds"] == -1
                else runtime_env_config["setup_timeout_seconds"]
            )

            start = time.perf_counter()
            (
                successful,
                serialized_context,
                error_message,
            ) = await _create_runtime_env_with_retry(
                runtime_env,
                setup_timeout_seconds,
                runtime_env_config,
            )
            creation_time_ms = int(round((time.perf_counter() - start) * 1000, 0))
            if not successful:
                # Recover the reference.
                self._reference_table.decrease_reference(
                    runtime_env, serialized_env, request.source_process
                )
            # Add the result to env cache.
            self._env_cache[serialized_env] = CreatedEnvResult(
                successful,
                serialized_context if successful else error_message,
                creation_time_ms,
            )
            # Reply the RPC
            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_OK
                if successful
                else agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                serialized_runtime_env_context=serialized_context,
                error_message=error_message,
            )

    async def DeleteRuntimeEnvIfPossible(self, request):
        self._logger.info(
            f"Got request from {request.source_process} to decrease "
            "reference for runtime env: "
            f"{request.serialized_runtime_env}."
        )

        try:
            runtime_env = RuntimeEnv.deserialize(request.serialized_runtime_env)
        except Exception as e:
            self._logger.exception(
                "[Decrease] Failed to parse runtime env: "
                f"{request.serialized_runtime_env}"
            )
            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message="".join(
                    traceback.format_exception(type(e), e, e.__traceback__)
                ),
            )

        try:
            self._reference_table.decrease_reference(
                runtime_env, request.serialized_runtime_env, request.source_process
            )
        except Exception as e:
            return runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=f"Fails to decrement reference for runtime env for {str(e)}",
            )

        return runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK
        )

    async def GetRuntimeEnvsInfo(self, request):
        """Return the runtime env information of the node."""
        # TODO(sang): Currently, it only includes runtime_env information.
        # We should include the URI information which includes,
        # URIs
        # Caller
        # Ref counts
        # Cache information
        # Metrics (creation time & success)
        # Deleted URIs
        limit = request.limit if request.HasField("limit") else -1
        runtime_env_states = defaultdict(ProtoRuntimeEnvState)
        runtime_env_refs = self._reference_table.runtime_env_refs
        for runtime_env, ref_cnt in runtime_env_refs.items():
            runtime_env_states[runtime_env].runtime_env = runtime_env
            runtime_env_states[runtime_env].ref_cnt = ref_cnt
        for runtime_env, result in self._env_cache.items():
            runtime_env_states[runtime_env].runtime_env = runtime_env
            runtime_env_states[runtime_env].success = result.success
            if not result.success:
                runtime_env_states[runtime_env].error = result.result
            runtime_env_states[runtime_env].creation_time_ms = result.creation_time_ms

        reply = runtime_env_agent_pb2.GetRuntimeEnvsInfoReply()
        count = 0
        for runtime_env_state in runtime_env_states.values():
            if limit != -1 and count >= limit:
                break
            count += 1
            reply.runtime_env_states.append(runtime_env_state)
        reply.total = len(runtime_env_states)
        return reply
