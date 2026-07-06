"""In-process pre-routing tokenization mirroring vLLM's ``/tokenize`` endpoint.

The KV-aware router needs prompt token ids before replica selection. Fetching
them over a per-request ``/tokenize`` RPC contends with inference on the
replica processes and collapses under concurrency, so the ingress tokenizes
in-process. To keep routing token ids byte-identical to the engine's prefill
tokens, this module does not re-implement tokenization: it builds the same
vLLM renderer the engine builds (``renderer_from_config``) and replays
``OpenAIServingTokenization.create_tokenize`` step by step on it.
"""

from typing import Any, Dict, List, Optional, Union

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    TokenizeChatRequest,
    TokenizeCompletionRequest,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

# choose_replica kwarg carrying the prompt token IDs to KV-aware routers.
REQUEST_TOKEN_IDS_KWARG = "request_token_ids"

# The vLLM api server defaults mirrored by the replica's backend server. The
# replica does not override any of them today; keep in sync with
# ``vllm.entrypoints.serve`` argument defaults.
_DEFAULT_CHAT_TEMPLATE = None
_DEFAULT_CHAT_TEMPLATE_CONTENT_FORMAT = "auto"
_DEFAULT_CHAT_TEMPLATE_KWARGS: Dict[str, Any] = {}
_TRUST_REQUEST_CHAT_TEMPLATE = False


class TokenizeError(Exception):
    """The request was rejected the same way ``/tokenize`` would reject it.

    Carries the HTTP ``status_code``, ``message`` and error ``type``.
    """

    def __init__(self, message: str, *, status_code: int, type: str):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.type = type


def build_tokenize_request(
    payload: Dict[str, Any]
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


class Tokenizer:
    """Tokenizes incoming requests with the engine's own vLLM renderer.

    Construction resolves the deployment's ``VllmConfig`` (CPU-only, same
    helper the engine uses) and builds the renderer from it, so the tokenizer,
    chat template, and truncation behavior are exactly the engine's. Raises on
    construction failure: KV-aware routing without token ids silently degrades
    to load balancing, so the replica must not come up half-configured.

    Args:
        llm_config: The deployment's LLM config.
    """

    def __init__(self, llm_config: LLMConfig):
        from vllm.renderers import renderer_from_config

        from ray.llm._internal.serve.engines.vllm.vllm_engine import (
            _get_vllm_engine_config,
        )

        _, vllm_config = _get_vllm_engine_config(llm_config)
        self._model_config = vllm_config.model_config
        self._renderer = renderer_from_config(vllm_config)
        logger.info(
            "In-process pre-routing tokenizer ready for %s",
            self._model_config.model,
        )

    async def tokenize(self, payload: Dict[str, Any]) -> Optional[List[int]]:
        """Tokenize a request ``payload`` into prompt token IDs.

        Mirrors ``OpenAIServingTokenization.create_tokenize``: chat requests go
        through ``preprocess_chat`` semantics, completion requests through
        ``preprocess_completion`` semantics, and token ids are extracted from
        the resulting engine inputs the same way.

        Args:
            payload: The request body, already parsed into a dict by ``LLMRouter``.

        Returns:
            The prompt token IDs, or ``None`` for bodies that are not routed on.

        Raises:
            TokenizeError: The request was rejected (same statuses ``/tokenize``
                returns).
        """
        from vllm.renderers.inputs.preprocess import extract_prompt_components

        request = build_tokenize_request(payload)
        if request is None:
            return None

        try:
            if isinstance(request, TokenizeChatRequest):
                engine_inputs = await self._render_chat(request)
            else:
                engine_inputs = await self._render_completion(request)
        except TokenizeError:
            raise
        except ValueError as e:
            # vLLM raises ValueError for invalid inputs; /tokenize maps these
            # to 400s. Mirror that mapping.
            raise TokenizeError(str(e), status_code=400, type="BadRequestError")

        input_ids: List[int] = []
        for engine_input in engine_inputs:
            components = extract_prompt_components(self._model_config, engine_input)
            if components.token_ids is not None:
                input_ids.extend(components.token_ids)
        return input_ids

    async def _render_chat(self, request: TokenizeChatRequest):
        """Replays ``OpenAIServingRender.preprocess_chat`` for tokenization."""
        from vllm.renderers.params import merge_kwargs
        from vllm.utils.mistral import is_mistral_tokenizer

        # Same refusal as OpenAIServing._validate_chat_template with the
        # backend's default trust_request_chat_template=False.
        if not _TRUST_REQUEST_CHAT_TEMPLATE and (
            request.chat_template is not None
            or (
                request.chat_template_kwargs
                and request.chat_template_kwargs.get("chat_template") is not None
            )
        ):
            raise TokenizeError(
                "Chat template is passed with request, but "
                "--trust-request-chat-template is not set. "
                "Refused request with untrusted chat template.",
                status_code=400,
                type="BadRequestError",
            )

        tool_dicts = (
            None
            if request.tools is None
            else [tool.model_dump() for tool in request.tools]
        )
        mm_config = self._model_config.multimodal_config
        default_template_kwargs = merge_kwargs(
            _DEFAULT_CHAT_TEMPLATE_KWARGS,
            dict(
                tools=tool_dicts,
                tokenize=(
                    is_mistral_tokenizer(self._renderer.tokenizer)
                    or self._model_config.enable_prompt_embeds
                ),
            ),
        )

        tok_params = request.build_tok_params(self._model_config)
        chat_params = request.build_chat_params(
            _DEFAULT_CHAT_TEMPLATE, _DEFAULT_CHAT_TEMPLATE_CONTENT_FORMAT
        ).with_defaults(
            default_template_kwargs,
            default_media_io_kwargs=(mm_config.media_io_kwargs if mm_config else None),
            default_mm_processor_kwargs=getattr(request, "mm_processor_kwargs", None),
        )

        _, (engine_input,) = await self._renderer.render_chat_async(
            [request.messages],
            chat_params,
            tok_params,
            prompt_extras={
                k: v
                for k in ("mm_processor_kwargs", "cache_salt")
                if (v := getattr(request, k, None)) is not None
            },
            skip_mm_cache=True,
        )
        return [engine_input]

    async def _render_completion(self, request: TokenizeCompletionRequest):
        """Replays ``OpenAIServingRender.preprocess_completion``."""
        from vllm.renderers.inputs.preprocess import parse_model_prompt, prompt_to_seq

        prompts = [
            parse_model_prompt(self._model_config, prompt)
            for prompt in prompt_to_seq(request.prompt)
        ]
        tok_params = request.build_tok_params(self._model_config)
        return await self._renderer.render_cmpl_async(
            prompts,
            tok_params,
            prompt_extras={
                k: v
                for k in ("mm_processor_kwargs", "cache_salt")
                if (v := getattr(request, k, None)) is not None
            },
            skip_mm_cache=True,
        )
