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
    """Build the request the engine renders the prompt from, so routing ids
    match the prefill tokens. Chat bodies build the full ``ChatCompletionRequest``
    so ``render_chat`` can drive the engine's own path across model families (HF
    chat template, Harmony for gpt_oss, Mistral).

    Convert Anthropic bodies with vLLM's ``/v1/messages`` converter.
    ``merge_inline_system`` must match the engine's template-derived setting.

    Select the schema by API path: the same body can produce different prompts
    under different APIs. Paths may include the Serve application's route prefix.

    Returns ``None`` for missing or unsupported paths, invalid bodies, or
    completions without a single string prompt. The caller falls back to
    token-less routing.

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
        # Token counting and other endpoints don't run a generation prefill.
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
            # Match the engine's tool config so render_chat handles tool requests
            # the same way (a no-op unless the deployment enables tool calling).
            enable_auto_tools=frontend_args.enable_auto_tool_choice,
            exclude_tools_when_tool_choice_none=(
                frontend_args.exclude_tools_when_tool_choice_none
            ),
            tool_parser=frontend_args.tool_call_parser,
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
            if isinstance(request, CompletionRequest):
                result = await self._renderer.render_completion(
                    request, skip_mm_cache=True
                )
            else:
                result = await self._renderer.render_chat(request, skip_mm_cache=True)
            if isinstance(result, ErrorResponse):
                raise TokenizeError(
                    result.error.message,
                    status_code=result.error.code,
                    type=result.error.type,
                )
        except TokenizeError:
            raise
        except (ValueError, VLLMClientError, jinja2.TemplateError) as e:
            # vLLM maps bad inputs and chat-template errors to 400; other
            # exceptions are real bugs and should surface, not degrade routing.
            raise TokenizeError(str(e), status_code=400, type="BadRequestError")

        rendered_inputs = (
            result if isinstance(request, CompletionRequest) else result[1]
        )
        input_ids: List[int] = []
        for rendered_input in rendered_inputs:
            components = extract_prompt_components(self._model_config, rendered_input)
            if components.token_ids is not None:
                input_ids.extend(components.token_ids)
        return input_ids
