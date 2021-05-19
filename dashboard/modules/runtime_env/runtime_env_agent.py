import json
import logging

from ray.core.generated import runtime_env_agent_pb2
from ray.core.generated import runtime_env_agent_pb2_grpc
from ray.core.generated import agent_manager_pb2
import ray.new_dashboard.utils as dashboard_utils
import ray._private.runtime_env as runtime_env
from ray._private.runtime_env import RuntimeEnvDict

logger = logging.getLogger(__name__)


class RuntimeEnvAgent(dashboard_utils.DashboardAgentModule,
                      runtime_env_agent_pb2_grpc.RuntimeEnvServiceServicer):
    """A rpc server to create or delete runtime envs.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config
    """

    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._resource_dir = dashboard_agent.resource_dir
        runtime_env.PKG_DIR = dashboard_agent.resource_dir

    async def CreateRuntimeEnv(self, request, context):
        runtime_env_dict: RuntimeEnvDict = json.loads(
            request.serialized_runtime_env or "{}")
        uris = runtime_env_dict.get("uris")
        if uris:
            logger.info("Create runtime env with uris %s", repr(uris))
            # TODO(guyang.sgy): Try `ensure_runtime_env_setup(uris)`
            # to don't packages.
            # But we don't initailize internal kv in agent now.
            pass

        return runtime_env_agent_pb2.CreateRuntimeEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK)

    async def DeleteRuntimeEnv(self, request, context):
        return runtime_env_agent_pb2.DeleteRuntimeEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK)

    async def run(self, server):
        runtime_env_agent_pb2_grpc.add_RuntimeEnvServiceServicer_to_server(
            self, server)
