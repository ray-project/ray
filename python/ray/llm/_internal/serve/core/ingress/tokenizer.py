import json
from typing import List, Optional

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

        gen = self._handle.options(stream=True).tokenize.remote(tok_req, None)
        resp = await gen.__anext__()
        if isinstance(resp, ErrorResponse):
            raise TokenizeError(
                resp.error.message,
                status_code=resp.error.code,
                type=resp.error.type,
            )
        return list(resp.tokens)

    def _build_tokenize_request(
        self, request_body: bytes, body_truncated: bool
    ) -> Optional[object]:
        """Build the Tokenize* request for ``request_body``.

        TODO (jeffreywang): Investigate these are unsupported.
        Returns ``None`` for bodies we don't route on:
        - Truncated/empty body
        - Non-string (multi-prompt) ``prompt``
        - Request with neither ``messages`` nor ``prompt``
        """
        try:
            if body_truncated or not request_body:
                return None

            payload = json.loads(request_body)
            if not isinstance(payload, dict):
                return None

            model = payload.get("model")
            if "messages" in payload:
                return TokenizeChatRequest.model_validate(
                    {
                        "model": model,
                        "messages": payload["messages"],
                        "add_generation_prompt": True,
                    }
                )
            if "prompt" in payload:
                prompt = payload["prompt"]
                if not isinstance(prompt, str):
                    # Multi-prompt (list) tokenization is out of scope; fall
                    # back to normal routing.
                    return None
                return TokenizeCompletionRequest.model_validate(
                    {
                        "model": model,
                        "prompt": prompt,
                        "add_special_tokens": True,
                    }
                )
            return None
        except Exception as e:
            logger.debug("Unsupported tokenize request, falling back: %s", e)
            return None
