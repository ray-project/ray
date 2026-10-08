from typing import Any, Dict, List, Optional, Union

import jinja2
from pydantic import ValidationError
from vllm.entrypoints.anthropic.protocol import AnthropicMessagesRequest
from vllm.entrypoints.anthropic.serving import AnthropicServingMessages
from vllm.entrypoints.chat_utils import load_chat_template
from vllm.entrypoints.launchers.cli_args import FrontendArgs
from vllm.entrypoints.openai.chat_completion.protocol import ChatCompletionRequest
from vllm.entrypoints.openai.completion.protocol import CompletionRequest
from vllm.entrypoints.serve.engine.protocol import ErrorResponse
from vllm.exceptions import VLLMClientError, VLLMValidationError
from vllm.renderers import renderer_from_config
from vllm.renderers.inputs.preprocess import extract_prompt_components
from vllm.renderers.online_renderer import OnlineRenderer

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    _get_vllm_engine_config,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


class TokenizeError(Exception):
    """A client error from vLLM's native prompt renderer.

    Carries the HTTP ``status_code``, ``message`` and error ``type``.
    """

    def __init__(self, message: str, *, status_code: int, type: str):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.type = type


def build_tokenize_request(
    payload: Dict[str, Any],
    *,
    merge_inline_system: bool = True,
    request_path: Optional[str] = None,
) -> Optional[Union[ChatCompletionRequest, CompletionRequest]]:
    """Validate the body using the API path and return a vLLM rendering request.

    Convert Anthropic messages to chat with the engine's ``merge_inline_system``
    setting. The path may include a Serve application prefix.

    Return ``None`` for missing or unsupported paths, invalid bodies, and
    completions without a single string prompt, so routing falls back without
    prompt tokens.

    TODO (jeffreywang): Support multi-prompt tokenization.
    """
    if request_path is None:
        return None
    try:
        if request_path.endswith("/v1/messages"):
            return AnthropicServingMessages.to_chat_completion_request(
                AnthropicMessagesRequest.model_validate(payload),
                merge_inline_system=merge_inline_system,
            )
        if request_path.endswith("/v1/chat/completions"):
            return ChatCompletionRequest.model_validate(payload)
        if request_path.endswith("/v1/completions"):
            request = CompletionRequest.model_validate(payload)
            if not isinstance(request.prompt, str) or request.prompt_embeds is not None:
                return None
            return request
        # Only the generation endpoints above support prompt-token routing here.
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

    async def tokenize(
        self, payload: Dict[str, Any], *, request_path: Optional[str] = None
    ) -> Optional[List[int]]:
        """Tokenize a request ``payload`` into prompt token IDs.

        Args:
            payload: The request body, already parsed into a dict by ``LLMRouter``.
            request_path: The original API path forwarded by HAProxy, if present.

        Returns:
            The prompt token IDs, or ``None`` for bodies that are not routed on.

        Raises:
            TokenizeError: vLLM's prompt renderer rejected the request.
        """
        request = build_tokenize_request(
            payload,
            merge_inline_system=self._merge_inline_system,
            request_path=request_path,
        )
        if request is None:
            return None

        try:
            if isinstance(request, ChatCompletionRequest):
                rendered_inputs = await self._render_chat(request)
            else:
                rendered_inputs = await self._render_completion(request)
        except TokenizeError:
            raise
        except (ValueError, VLLMClientError, jinja2.TemplateError) as e:
            # vLLM maps bad inputs and chat-template errors to 400; other
            # exceptions are real bugs and should surface, not degrade routing.
            raise TokenizeError(str(e), status_code=400, type="BadRequestError")

        input_ids: List[int] = []
        for rendered_input in rendered_inputs:
            components = extract_prompt_components(self._model_config, rendered_input)
            if components.token_ids is not None:
                input_ids.extend(components.token_ids)
        return input_ids

    async def _render_chat(self, request: ChatCompletionRequest):
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

    async def _render_completion(self, request: CompletionRequest):
        return await self._renderer.preprocess_completion(
            request,
            prompt_input=request.prompt,
            prompt_embeds=None,
            skip_mm_cache=True,
        )
