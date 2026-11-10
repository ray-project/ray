import asyncio
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

logger = get_logger(__name__)


class TokenizeError(Exception):
    """A client error from prompt rendering."""

    def __init__(self, message: str, *, status_code: int, type: str):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.type = type


class KVRouterState:
    """Tokenizer, tracker and token channel for one KV-aware target deployment."""

    def __init__(self, llm_config: "LLMConfig"):
        # Keep vLLM and pyzmq off the non-KV router import path.
        from ray.llm._internal.serve.routing_policies.kv_aware import token_channel
        from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (  # noqa: E501
            Tokenizer,
        )

        self._llm_config = llm_config
        self.tracker = None
        self.token_sender = token_channel.TokenSender()
        self.tokenizer_task = asyncio.create_task(
            asyncio.to_thread(Tokenizer, llm_config)
        )
        self._warned_no_token_endpoint = False

    def start_tracking(
        self,
        deployment_id: Any,
        peer_handle: Any,
        model_id: Optional[str] = None,
    ) -> None:
        """Register and subscribe before initializing the target handle.

        Run synchronously before the event loop processes replica updates.
        """
        from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
            build_kv_token_tracker,
        )

        self.tracker = build_kv_token_tracker(self._llm_config, deployment_id)
        self.tracker.start_replica_broadcast(peer_handle, model_id=model_id)
        self.tracker.subscribe_token_endpoints(self.token_sender.connect)

    def check_health(self) -> None:
        if self.tokenizer_task.done():
            self.tokenizer_task.result()

    async def tokenize(
        self,
        routing_payload: Optional[SimpleNamespace],
        *,
        request_path: Optional[str],
    ) -> Optional[List[int]]:
        if routing_payload is None:
            return None
        tokenizer = await asyncio.shield(self.tokenizer_task)
        return await tokenizer.tokenize(
            vars(routing_payload), request_path=request_path
        )

    def stage_prompt_tokens(
        self,
        replica_id: str,
        token_endpoint: Optional[str],
        request_token_ids: Optional[List[int]],
        routing_payload: Optional[SimpleNamespace],
    ) -> Dict[str, str]:
        if not request_token_ids or routing_payload is None:
            return {}
        from ray.llm._internal.serve.routing_policies.kv_aware.vllm.prompt_token_forwarding import (
            is_text_only_chat,
        )

        if not is_text_only_chat(getattr(routing_payload, "messages", [])):
            return {}
        if not token_endpoint:
            if not self._warned_no_token_endpoint:
                self._warned_no_token_endpoint = True
                logger.warning(
                    "Selected replica %s did not advertise a prompt-token "
                    "ZMQ endpoint; falling back to engine tokenization.",
                    replica_id,
                )
            return {}

        from ray.llm._internal.serve.routing_policies.kv_aware import token_channel

        key = uuid.uuid4().hex
        try:
            payload = token_channel.encode_prompt_token_ids(request_token_ids)
        except Exception as e:
            logger.warning(
                "Failed to encode prompt token IDs for selected replica %s; "
                "falling back to engine tokenization: %s",
                replica_id,
                e,
            )
            return {}
        if not self.token_sender.push(token_endpoint, key, payload):
            return {}
        return {KV_TOKEN_KEY_HEADER: key}

    def close(self) -> None:
        self.token_sender.close()
