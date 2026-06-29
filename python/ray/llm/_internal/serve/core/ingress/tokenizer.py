from typing import Any, Dict, List, Optional, Union

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ErrorResponse,
    TokenizeChatRequest,
    TokenizeCompletionRequest,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

# choose_replica kwarg carrying the prompt token IDs to KV-aware routers.
REQUEST_TOKEN_IDS_KWARG = "request_token_ids"


class TokenizeError(Exception):
    """The ``/tokenize`` endpoint rejected the request.

    Carries vLLM's HTTP ``status_code``, ``message`` and error ``type``.
    """

    def __init__(self, message: str, *, status_code: int, type: str):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.type = type


class Tokenizer:
    """Tokenizes incoming requests via the replica's ``/tokenize`` endpoint.

    Args:
        handle: A handle to the LLMServer deployment.
    """

    def __init__(self, handle: DeploymentHandle):
        self._handle = handle

    async def tokenize(self, payload: Dict[str, Any]) -> Optional[List[int]]:
        """Tokenize a request ``payload`` into prompt token IDs.

        Args:
            payload: The request body, already parsed into a dict by ``LLMRouter``.

        Returns:
            The prompt token IDs, or ``None`` for bodies that are not routed on.

        Raises:
            TokenizeError: The ``/tokenize`` endpoint rejected the request.
        """
        tok_req = self._build_tokenize_request(payload)
        if tok_req is None:
            return None

        # /tokenize yields a single response; drain the stream fully so the
        # handle response is cleaned up.
        resp = None
        async for chunk in self._handle.options(stream=True).tokenize.remote(
            tok_req, None
        ):
            resp = chunk
        if resp is None:
            raise TokenizeError(
                "/tokenize returned no response",
                status_code=500,
                type="internal_error",
            )
        if isinstance(resp, ErrorResponse):
            raise TokenizeError(
                resp.error.message,
                status_code=resp.error.code,
                type=resp.error.type,
            )
        return list(resp.tokens)

    def _build_tokenize_request(
        self, payload: Dict[str, Any]
    ) -> Optional[Union[TokenizeChatRequest, TokenizeCompletionRequest]]:
        """Build the Tokenize* request for ``payload``.

        KV-aware routing sends each request to one replica, scored on a single
        prompt's token sequence, so we return ``None`` (the caller falls back to
        token-less routing) for bodies that don't have exactly one prompt:
        - A non-string ``prompt``: an OpenAI *batch* completion where ``prompt``
          is a list, e.g. ``{"prompt": ["q1", "q2"]}`` (or pre-tokenized id
          lists). N prompts give N token sequences, so there's no single key to
          route the one request on.

        TODO (jeffreywang): Support multi-prompt tokenization.
        """
        try:
            if "messages" in payload:
                # Forward every request field the engine renders the prompt from
                # so the routing token IDs match the prefill tokens.
                return TokenizeChatRequest.model_validate(
                    {
                        k: v
                        for k, v in payload.items()
                        if k in TokenizeChatRequest.model_fields
                    }
                )
            if "prompt" in payload:
                if not isinstance(payload["prompt"], str):
                    # TODO (jeffreywang): Multi-prompt (list) tokenization is unsupported;
                    # fall back to token-less routing.
                    return None
                return TokenizeCompletionRequest.model_validate(
                    {
                        k: v
                        for k, v in payload.items()
                        if k in TokenizeCompletionRequest.model_fields
                    }
                )
            # Should be unreachable: LLMRouter only routes bodies with messages
            # or a prompt (see _parse_routing_payload).
            logger.warning(
                "Tokenizer got a payload with neither messages nor prompt; "
                "falling back to token-less routing."
            )
            return None
        except Exception as e:
            logger.debug("Unsupported tokenize request, falling back: %s", e)
            return None
