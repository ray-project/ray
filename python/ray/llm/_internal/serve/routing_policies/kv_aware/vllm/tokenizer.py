from typing import Any, Dict, List, Optional, Union

import jinja2
from pydantic import ValidationError
from vllm.entrypoints.chat_utils import load_chat_template
from vllm.entrypoints.openai.cli_args import FrontendArgs
from vllm.entrypoints.openai.engine.protocol import ErrorResponse
from vllm.exceptions import VLLMClientError
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


def build_tokenize_request(
    payload: Dict[str, Any],
) -> Optional[Union[ChatCompletionRequest, TokenizeCompletionRequest]]:
    """Build the request the engine renders the prompt from, so routing ids
    match the prefill tokens. Chat bodies build the full ``ChatCompletionRequest``
    so ``render_chat`` can drive the engine's own path across model families (HF
    chat template, Harmony for gpt_oss, Mistral).

    Returns ``None`` (caller falls back to token-less routing) for a body with
    no single string prompt, e.g. a batch ``prompt`` list, since KV-aware
    routing scores one request on one token sequence.

    TODO (jeffreywang): Support multi-prompt tokenization.
    """
    try:
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
    except ValidationError as e:
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
        self._renderer = OnlineRenderer(
            self._model_config,
            renderer_from_config(vllm_config),
            request_logger=None,
            chat_template=load_chat_template(frontend_args.chat_template),
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

    async def tokenize(self, payload: Dict[str, Any]) -> Optional[List[int]]:
        """Tokenize a request ``payload`` into prompt token IDs.

        Args:
            payload: The request body, already parsed into a dict by ``LLMRouter``.

        Returns:
            The prompt token IDs, or ``None`` for bodies that are not routed on.

        Raises:
            TokenizeError: The ``/tokenize`` endpoint rejected the request.
        """
        request = build_tokenize_request(payload)
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
            # /tokenize maps bad inputs and chat-template errors to 400; other
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

    async def _render_completion(self, request: TokenizeCompletionRequest):
        return await self._renderer.preprocess_completion(
            request,
            prompt_input=request.prompt,
            prompt_embeds=None,
            skip_mm_cache=True,
        )
