import json
from typing import List, Optional, Union

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

    async def tokenize(
        self, request_body: bytes, body_truncated: bool
    ) -> Optional[List[int]]:
        """Tokenize ``request_body`` into prompt token IDs.

        Args:
            request_body: The raw request body to tokenize.
            body_truncated: Whether ``request_body`` was truncated upstream.

        Returns:
            The prompt token IDs, or ``None`` for bodies that are not routed on.

        Raises:
            TokenizeError: The ``/tokenize`` endpoint rejected the request.
        """
        tok_req = self._build_tokenize_request(request_body, body_truncated)
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
        self, request_body: bytes, body_truncated: bool
    ) -> Optional[Union[TokenizeChatRequest, TokenizeCompletionRequest]]:
        """Build the Tokenize* request for ``request_body``.

        Returns ``None`` for bodies we don't route on (the caller falls back to
        token-less routing):
        - Truncated/empty body
        - Non-string (multi-prompt) ``prompt``
        - Request with neither ``messages`` nor ``prompt``

        TODO (jeffreywang): Support multi-prompt tokenization instead of dropping it.
        """
        try:
            if body_truncated or not request_body:
                return None

            payload = json.loads(request_body)
            if not isinstance(payload, dict):
                return None

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
                    # fall back to normal routing.
                    return None
                return TokenizeCompletionRequest.model_validate(
                    {
                        k: v
                        for k, v in payload.items()
                        if k in TokenizeCompletionRequest.model_fields
                    }
                )
            return None
        except Exception as e:
            logger.debug("Unsupported tokenize request, falling back: %s", e)
            return None
