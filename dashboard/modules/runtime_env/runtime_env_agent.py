import asyncio
import json
import logging

from ray.core.generated import runtime_env_agent_pb2
from ray.core.generated import runtime_env_agent_pb2_grpc
from ray.core.generated import agent_manager_pb2
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.runtime_env.runtime_env_consts \
    as runtime_env_consts
import ray._private.runtime_env as runtime_env
from ray._private.utils import import_attr
from ray.workers.pluggable_runtime_env import RuntimeEnvContext

logger = logging.getLogger(__name__)


class RuntimeEnvAgent(dashboard_utils.DashboardAgentModule,
                      runtime_env_agent_pb2_grpc.RuntimeEnvServiceServicer):
    """A rpc server to create or delete runtime envs.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config.
    """

    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._session_dir = dashboard_agent.session_dir
        self._runtime_env_dir = dashboard_agent.runtime_env_dir
        self._setup = import_attr(dashboard_agent.runtime_env_setup_hook)
        runtime_env.PKG_DIR = dashboard_agent.runtime_env_dir

    async def CreateRuntimeEnv(self, request, context):
        async def _setup_runtime_env(serialized_runtime_env, session_dir):
            loop = asyncio.get_event_loop()
            runtime_env: dict = json.loads(serialized_runtime_env or "{}")
            return await loop.run_in_executor(None, self._setup, runtime_env,
                                              session_dir)

        logger.info("Creating runtime env: %s.",
                    request.serialized_runtime_env)
        runtime_env_dict = json.loads(request.serialized_runtime_env or "{}")
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
                    request.serialized_runtime_env, self._session_dir)
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
            return runtime_env_agent_pb2.CreateRuntimeEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=error_message)

        serialized_context = runtime_env_context.serialize()
        logger.info("Successfully created runtime env: %s, the context: %s",
                    request.serialized_runtime_env, serialized_context)
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
