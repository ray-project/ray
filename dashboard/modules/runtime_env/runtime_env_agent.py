import asyncio
from dataclasses import dataclass
import json
import logging
from ray._private.ray_logging import setup_component_logger
from typing import Dict

from ray.core.generated import runtime_env_agent_pb2
from ray.core.generated import runtime_env_agent_pb2_grpc
from ray.core.generated import agent_manager_pb2
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.runtime_env.runtime_env_consts \
    as runtime_env_consts
import ray._private.runtime_env as runtime_env
from ray._private.utils import import_attr
from ray.workers.pluggable_runtime_env import (RuntimeEnvContext,
                                               using_thread_local_logger)

logger = logging.getLogger(__name__)


@dataclass
class CreatedEnvResult:
    # Whether or not the env was installed correctly.
    success: bool
    # If success is True, will be a serialized RuntimeEnvContext
    # If success is False, will be an error message.
    result: str


class RuntimeEnvAgent(dashboard_utils.DashboardAgentModule,
                      runtime_env_agent_pb2_grpc.RuntimeEnvServiceServicer):
    """An RPC server to create and delete runtime envs.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config.
    """

    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._session_dir = dashboard_agent.session_dir
        self._runtime_env_dir = dashboard_agent.runtime_env_dir
        self._setup = import_attr(dashboard_agent.runtime_env_setup_hook)
        self._logging_params = dashboard_agent.logging_params
        self._per_job_logger_cache = dict()
        runtime_env.PKG_DIR = dashboard_agent.runtime_env_dir
        # Cache the results of creating envs to avoid repeatedly calling into
        # conda and other slow calls.
        self._env_cache: Dict[str, CreatedEnvResult] = dict()

    def get_or_create_logger(self, job_id: bytes):
        job_id = job_id.decode()
        if job_id not in self._per_job_logger_cache:
            params = self._logging_params.copy()
            params["filename"] = f"runtime_env_setup-{job_id}.log"
            params["logger_name"] = f"runtime_env_{job_id}"
            per_job_logger = setup_component_logger(**params)
            self._per_job_logger_cache[job_id] = per_job_logger
        return self._per_job_logger_cache[job_id]

    async def CreateRuntimeEnv(self, request, context):
        async def _setup_runtime_env(serialized_runtime_env, session_dir):
            # This function will be ran inside a thread
            def run_setup_with_logger():
                runtime_env: dict = json.loads(serialized_runtime_env or "{}")
                per_job_logger = self.get_or_create_logger(request.job_id)
                # Here we set the logger context for the setup hook execution.
                # The logger needs to be thread local because there can be
                # setup hooks ran for arbitrary job in arbitrary threads.
                with using_thread_local_logger(per_job_logger):
                    env_context = self._setup(runtime_env, session_dir)
                return env_context

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, run_setup_with_logger)

        serialized_env = request.serialized_runtime_env
        if serialized_env in self._env_cache:
            serialized_context = self._env_cache[serialized_env]
            result = self._env_cache[serialized_env]
            if result.success:
                context = result.result
                logger.info("Runtime env already created successfully. "
                            f"Env: {serialized_env}, context: {context}")
                return runtime_env_agent_pb2.CreateRuntimeEnvReply(
                    status=agent_manager_pb2.AGENT_RPC_STATUS_OK,
                    serialized_runtime_env_context=context)
            else:
                error_message = result.result
                logger.info("Runtime env already failed. "
                            f"Env: {serialized_env}, err: {error_message}")
                return runtime_env_agent_pb2.CreateRuntimeEnvReply(
                    status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                    error_message=error_message)

        logger.info(f"Creating runtime env: {serialized_env}")
        runtime_env_dict = json.loads(serialized_env or "{}")
        uris = runtime_env_dict.get("uris")
        runtime_env_context: RuntimeEnvContext = None
        error_message = None
        for _ in range(runtime_env_consts.RUNTIME_ENV_RETRY_TIMES):
            try:
                if uris:
                    # TODO(guyang.sgy): Try `ensure_runtime_env_setup(uris)`
                    # to download packages.
                    # But we don't initailize internal kv in agent now.
                    pass
                runtime_env_context = await _setup_runtime_env(
                    serialized_env, self._session_dir)
                break
            except Exception as ex:
                logger.exception("Runtime env creation failed.")
                error_message = str(ex)
                await asyncio.sleep(
                    runtime_env_consts.RUNTIME_ENV_RETRY_INTERVAL_MS / 1000)
        if error_message:
            logger.error(
                "Runtime env creation failed for %d times, "
                "don't retry any more.",
                runtime_env_consts.RUNTIME_ENV_RETRY_TIMES)
            self._env_cache[serialized_env] = CreatedEnvResult(
                False, error_message)
            return runtime_env_agent_pb2.CreateRuntimeEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=error_message)

        serialized_context = runtime_env_context.serialize()
        self._env_cache[serialized_env] = CreatedEnvResult(
            True, serialized_context)
        logger.info("Successfully created runtime env: %s, the context: %s",
                    serialized_env, serialized_context)
        return runtime_env_agent_pb2.CreateRuntimeEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK,
            serialized_runtime_env_context=serialized_context)

    async def DeleteRuntimeEnv(self, request, context):
        # TODO(guyang.sgy): Delete runtime env local files.
        return runtime_env_agent_pb2.DeleteRuntimeEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
            error_message="Not implemented.")

    async def run(self, server):
        runtime_env_agent_pb2_grpc.add_RuntimeEnvServiceServicer_to_server(
            self, server)
