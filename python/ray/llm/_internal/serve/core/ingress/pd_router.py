"""Direct-streaming ingress routing for KV-aware prefill/decode deployments."""

from types import SimpleNamespace
from typing import TYPE_CHECKING, Dict, List

from fastapi import Request

from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.core.protocol import IngressRoutingResponse
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    build_kv_token_tracker,
)
from ray.llm._internal.serve.routing_policies.kv_aware.pd_router import (
    PDRequestCoordinator,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
)
from ray.serve.handle import DeploymentHandle

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
    from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
        KVTokenTracker,
    )


class LLMPDRouter(LLMRouter):
    """Own P/D trackers and route requests through prefill before selecting decode.

    Inherits HTTP endpoints, token forwarding, and lifecycle handling from
    LLMRouter. The coordinator runs prefill; HAProxy carries the decode stream.
    """

    async def __init__(
        self,
        server: DeploymentHandle,
        llm_config: "LLMConfig",
        prefill_server: DeploymentHandle,
        prefill_config: "LLMConfig",
    ):
        self.prefill_server = prefill_server
        self.prefill_config = prefill_config
        self.decode_server = server
        await super().__init__(server, llm_config)
        prefill_server._init(_run_router_in_separate_loop=False)
        self.coordinator = PDRequestCoordinator(
            prefill_server,
            self.prefill_kv_token_tracker,
            self.decode_kv_token_tracker,
            self,
            self.route_decode,
        )

    def create_token_trackers(
        self, server: DeploymentHandle, llm_config: "LLMConfig"
    ) -> Dict[DeploymentID, "KVTokenTracker"]:
        self.prefill_kv_token_tracker = build_kv_token_tracker(
            self.prefill_config, self.prefill_server.deployment_id
        )
        self.decode_kv_token_tracker = build_kv_token_tracker(
            llm_config, server.deployment_id
        )
        return {
            self.prefill_server.deployment_id: self.prefill_kv_token_tracker,
            server.deployment_id: self.decode_kv_token_tracker,
        }

    async def route_request(self, request: Request) -> IngressRoutingResponse:
        return await self.coordinator.route(request, await request.body())

    async def route_decode(
        self, routing_payload: SimpleNamespace, token_ids: List[int], request_id: str
    ) -> IngressRoutingResponse:
        """Select D and stage tokens under the shared routing-attempt ID."""
        host, port, replica_id, token_endpoint = await self._pick_replica(
            self.decode_server,
            routing_payload,
            token_ids,
            routing_request_id=request_id,
        )
        self.push_prompt_tokens(
            token_endpoint=token_endpoint,
            replica_id=replica_id,
            request_token_ids=token_ids,
            token_key=request_id,
        )
        return {
            "host": host,
            "port": port,
            "replica_id": replica_id,
            RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD: {
                KV_TOKEN_KEY_HEADER: request_id
            },
        }
