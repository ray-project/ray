from typing import Any, Dict, List, Optional, Union

import jinja2
from pydantic import ValidationError
from vllm.entrypoints.anthropic.protocol import AnthropicMessagesRequest
from vllm.entrypoints.anthropic.serving import AnthropicServingMessages
from vllm.entrypoints.chat_utils import load_chat_template
from vllm.entrypoints.launchers.cli_args import FrontendArgs
from vllm.entrypoints.openai.chat_completion.protocol import (
    ChatCompletionRequest as VLLMChatCompletionRequest,
)
from vllm.entrypoints.serve.engine.protocol import ErrorResponse
from vllm.exceptions import VLLMClientError, VLLMValidationError
from vllm.renderers import renderer_from_config
from vllm.renderers.inputs.preprocess import extract_prompt_components
from vllm.renderers.online_renderer import OnlineRenderer

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    TokenizeCompletionRequest,
)
from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    _get_vllm_engine_config,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


class TokenizeError(Exception):
    """The request was rejected the same way vLLM's native ASGI route
    ``/tokenize`` would reject it.

    Carries the HTTP ``status_code``, ``message`` and error ``type``.
    """

    def __init__(self, message: str, *, status_code: int, type: str):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.type = type


# Content block types only the Anthropic Messages API has. OpenAI chat bodies
# carry tool calls on the message (``tool_calls``, role ``tool``) instead.
# ``thinking`` and ``tool_reference`` are left out: vLLM's OpenAI chat parser
# accepts those part types too.
_ANTHROPIC_ONLY_BLOCK_TYPES = frozenset(
    {"tool_use", "tool_result", "redacted_thinking"}
)


def is_anthropic_messages_payload(payload: Dict[str, Any]) -> bool:
    """Whether ``payload`` is an Anthropic Messages (``/v1/messages``) body.

    HAProxy forwards only the body to ``/internal/route``, not the request
    path, so the API is inferred from fields only an Anthropic body has: a
    top-level ``system`` prompt, tools declared with ``input_schema``, or
    Anthropic-only content blocks. Every Claude Code request has a system
    prompt and tools. A body with none of them (e.g. plain-text user turns) is
    also a valid OpenAI chat body and is tokenized as one.
    """
    if "system" in payload:
        return True
    tools = payload.get("tools")
    if isinstance(tools, list) and any(
        isinstance(tool, dict) and "input_schema" in tool for tool in tools
    ):
        return True
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return False
    for message in messages:
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type in _ANTHROPIC_ONLY_BLOCK_TYPES or (
                block_type == "image" and "source" in block
            ):
                return True
    return False


def build_tokenize_request(
    payload: Dict[str, Any],
    *,
    merge_inline_system: bool = True,
) -> Optional[Union[VLLMChatCompletionRequest, TokenizeCompletionRequest]]:
    """Build the request the engine renders the prompt from, so routing ids
    match the prefill tokens. Chat bodies build the full ``ChatCompletionRequest``
    so ``render_chat`` can drive the engine's own path across model families (HF
    chat template, Harmony for gpt_oss, Mistral).

    Anthropic Messages bodies go through the conversion vLLM's ``/v1/messages``
    handler runs before rendering, so they route on the prompt the engine
    prefills. ``merge_inline_system`` must match the engine's handler, which
    derives it from the deployment's chat template.

    Returns ``None`` (caller falls back to token-less routing) for a body with
    no single string prompt, e.g. a batch ``prompt`` list, since KV-aware
    routing scores one request on one token sequence.

    TODO (jeffreywang): Support multi-prompt tokenization.
    """
    try:
        if is_anthropic_messages_payload(payload):
            if "max_tokens" not in payload:
                # A /v1/messages/count_tokens body. It runs no prefill, so
                # there is nothing to score replicas on.
                return None
            return AnthropicServingMessages.to_chat_completion_request(
                AnthropicMessagesRequest.model_validate(payload),
                merge_inline_system=merge_inline_system,
            )
        if "messages" in payload:
            return ChatCompletionRequest.model_validate(
                {
                    k: v
                    for k, v in payload.items()
                    if k in ChatCompletionRequest.model_fields
                }
            )
        if "prompt" in payload:
            if not isinstance(payload["prompt"], str):
                return None
            return TokenizeCompletionRequest.model_validate(
                {
                    k: v
                    for k, v in payload.items()
                    if k in TokenizeCompletionRequest.model_fields
                }
            )
        # Unreachable: LLMRouter only routes bodies with messages or a prompt.
        logger.warning(
            "Tokenizer got a payload with neither messages nor prompt; "
            "falling back to token-less routing."
        )
        return None
    except (ValidationError, VLLMValidationError) as e:
        # vLLM's request validators can reject sampling params before prompt
        # rendering. Route without tokens so the engine returns its normal
        # client error; failing the router consultation would become a 500.
        logger.warning("Unsupported tokenize request, falling back: %s", e)
        return None


class Tokenizer:
    """Tokenizes requests with vLLM's ``OnlineRenderer``.

    Configured from the deployment's frontend args so the tokenizer, chat
    template, and trust policy match the engine's.

    Args:
        llm_config: The deployment's LLM config.
    """

    def __init__(self, llm_config: LLMConfig):
        engine_config = llm_config.get_engine_config()
        _, vllm_config = _get_vllm_engine_config(llm_config, device_type="cpu")
        self._model_config = vllm_config.model_config

        frontend_args = FrontendArgs(**engine_config.frontend_kwargs)
        chat_template = load_chat_template(frontend_args.chat_template)
        # The engine's /v1/messages handler derives this from the same template.
        self._merge_inline_system = (
            AnthropicServingMessages._detect_merge_inline_system(chat_template)
        )
        self._renderer = OnlineRenderer(
            self._model_config,
            renderer_from_config(vllm_config),
            request_logger=None,
            chat_template=chat_template,
            chat_template_content_format=frontend_args.chat_template_content_format,
            trust_request_chat_template=frontend_args.trust_request_chat_template,
            trust_request_mm_kwargs=frontend_args.trust_request_mm_kwargs,
            # Match the engine's tool config so render_chat handles tool requests
            # the same way (a no-op unless the deployment enables tool calling).
            enable_auto_tools=frontend_args.enable_auto_tool_choice,
            exclude_tools_when_tool_choice_none=(
                frontend_args.exclude_tools_when_tool_choice_none
            ),
            tool_parser=frontend_args.tool_call_parser,
            tool_strict_level=frontend_args.tool_strict_level,
            default_chat_template_kwargs=frontend_args.default_chat_template_kwargs,
        )
        logger.info(
            "In-process pre-routing tokenizer ready for %s",
            self._model_config.model,
        )

    async def tokenize(self, payload: Dict[str, Any]) -> Optional[List[int]]:
        """Tokenize a request ``payload`` into prompt token IDs.

        Args:
            payload: The request body, already parsed into a dict by ``LLMRouter``.

        Returns:
            The prompt token IDs, or ``None`` for bodies that are not routed on.

        Raises:
            TokenizeError: The ``/tokenize`` endpoint rejected the request.
        """
        request = build_tokenize_request(
            payload, merge_inline_system=self._merge_inline_system
        )
        if request is None:
            return None

        try:
            # Chat requests converted from an Anthropic body are vLLM's
            # ChatCompletionRequest, not Ray's subclass, so test for completion.
            if isinstance(request, TokenizeCompletionRequest):
                rendered_inputs = await self._render_completion(request)
            else:
                rendered_inputs = await self._render_chat(request)
        except TokenizeError:
            raise
        except (ValueError, VLLMClientError, jinja2.TemplateError) as e:
            # /tokenize maps bad inputs and chat-template errors to 400; other
            # exceptions are real bugs and should surface, not degrade routing.
            raise TokenizeError(str(e), status_code=400, type="BadRequestError")

        input_ids: List[int] = []
        for rendered_input in rendered_inputs:
            components = extract_prompt_components(self._model_config, rendered_input)
            if components.token_ids is not None:
                input_ids.extend(components.token_ids)
        return input_ids

    async def _render_chat(self, request: VLLMChatCompletionRequest):
        """Render a chat request to prompt inputs via the engine's own render_chat
        (HF template, Harmony for gpt_oss, Mistral; refuses untrusted templates)."""
        result = await self._renderer.render_chat(request, skip_mm_cache=True)
        if isinstance(result, ErrorResponse):
            raise TokenizeError(
                result.error.message,
                status_code=result.error.code,
                type=result.error.type,
            )
        _, rendered_inputs = result
        return rendered_inputs

    async def _render_completion(self, request: TokenizeCompletionRequest):
        return await self._renderer.preprocess_completion(
            request,
            prompt_input=request.prompt,
            prompt_embeds=None,
            skip_mm_cache=True,
        )
