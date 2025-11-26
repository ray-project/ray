import copy
import signal
import time
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    List,
    Optional,
    Union,
    Dict,
)

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
)

from ray.llm._internal.serve.constants import ENABLE_WORKER_PROCESS_SETUP_HOOK

class ChatRole(str, Enum):
    user = "user"
    assistant = "assistant"
    system = "system"


def format_messages_to_prompt(messages: List[Any]) -> str:
    prompt = "A conversation between a user and an assistant.\n"

    for message in messages:
        # Handle dicts (standard OpenAI format) or objects (if Ray passes wrappers)
        if isinstance(message, dict):
            role = message.get("role")
            content = message.get("content", "")
        else:
            # Fallback for object access if it's a Pydantic model
            role = getattr(message, "role", "user")
            content = getattr(message, "content", "") or ""

        role_str = str(role)

        if role_str == ChatRole.system.value:
            prompt += f"### System: {content.strip()}\n"
        elif role_str == ChatRole.user.value:
            prompt += f"### User: {content.strip()}\n"
        elif role_str == ChatRole.assistant.value:
            prompt += f"### Assistant: {content.strip()}\n"

    prompt += "### Assistant:"
    return prompt


class SGLangServer:
    def __init__(self, _llm_config: LLMConfig):

        self._llm_config = _llm_config
        self.engine_kwargs = _llm_config.engine_kwargs

        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e

        original_signal_func = signal.signal

        def noop_signal_handler(sig, action):
            # Returns default handler to satisfy signal.signal() return signature
            return signal.SIG_DFL

        try:
            # Override signal.signal with our no-op function
            signal.signal = noop_signal_handler
            self.engine = sglang.Engine(**self.engine_kwargs)
        finally:
            signal.signal = original_signal_func

    async def chat(
        self, request: ChatCompletionRequest
    ) -> AsyncGenerator[ChatCompletionResponse, None]:
        
        # Format prompts (handles dicts or objects in request.messages)
        prompt_string = format_messages_to_prompt(request.messages)

        temp = request.temperature
        if temp is None:
            temp = 0.7
        top_p = request.top_p
        if top_p is None:
            top_p = 1.0
        max_tokens = request.max_tokens
        if max_tokens is None:
            max_tokens = 128

        sampling_params = {
            "temperature": temp,
            "max_new_tokens": max_tokens,
            "stop": request.stop,
            "top_p": top_p,
        }

        raw = await self.engine.async_generate(
            prompt=prompt_string,
            sampling_params=sampling_params,
            stream=False,
        )

        if isinstance(raw, list):
            if not raw:
                raise RuntimeError("SGLang engine returned an empty response list during chat generation.")
            raw = raw[0]

        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}

        if isinstance(finish_reason_info, dict):
            finish_reason = finish_reason_info.get("type", "length")
        else:
            finish_reason = str(finish_reason_info)

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        usage_data = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

        choice_data = {
            "index": 0,
            "message": {
                "role": ChatRole.assistant.value,
                "content": text.strip()
            },
            "finish_reason": finish_reason,
        }

        resp = ChatCompletionResponse(
            id=meta.get("id", f"sglang-chat-{int(time.time())}"),
            object="chat.completion",
            created=int(time.time()),
            model=request.model,
            choices=[choice_data],
            usage=usage_data,
        )
        

        yield resp

    async def completions(self, request: CompletionRequest) -> AsyncGenerator[CompletionResponse, None]:
        prompt_input = request.prompt

        if isinstance(prompt_input, list):
            prompt_string = prompt_input[0]
        else:
            prompt_string = prompt_input

        temp = getattr(request, "temperature", None)
        if temp is None:
            temp = 0.7

        top_p = getattr(request, "top_p", None)
        if top_p is None:
            top_p = 1.0

        max_tokens = getattr(request, "max_tokens", None)
        if max_tokens is None:
            max_tokens = 128

        stop_sequences = getattr(request, "stop", None)

        sampling_params = {
            "temperature": temp,
            "max_new_tokens": max_tokens,
            "stop": stop_sequences,
            "top_p": top_p,
        }

        raw = await self.engine.async_generate(
            prompt=prompt_string,
            sampling_params=sampling_params,
            stream=False,
        )

        if isinstance(raw, list):
            if not raw:
                raise RuntimeError("SGLang engine returned an empty response list during text completion.")
            raw = raw[0]

        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}

        if isinstance(finish_reason_info, dict):
            finish_reason = finish_reason_info.get("type", "length")
        else:
            finish_reason = str(finish_reason_info)

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        usage_data = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

        choice_data = {
            "index": 0,
            "text": text,
            "logprobs": None,
            "finish_reason": finish_reason,
        }

        resp = CompletionResponse(
            id=meta.get("id", f"sglang-comp-{int(time.time())}"),
            object="text_completion",
            created=int(time.time()),
            model=getattr(request, "model", "default_model"),
            choices=[choice_data],
            usage=usage_data,
        )

        yield resp

    async def llm_config(self) -> Optional[LLMConfig]:
        return self._llm_config

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):

        deployment_options = copy.deepcopy(llm_config.deployment_config)
        pg_config = llm_config.placement_group_config or {}

        if "placement_group_bundles" not in pg_config:
            pg_bundles = [
                {"CPU": 1, "GPU": 1},
                {"GPU": 1},
            ]
            pg_strategy = "PACK"
        else:
            pg_bundles = pg_config.get("placement_group_bundles")
            pg_strategy = pg_config.get("placement_group_strategy", "PACK")

        deployment_options.update(
            {
                "placement_group_bundles": pg_bundles,
                "placement_group_strategy": pg_strategy,
            }
        )

        ray_actor_options = deployment_options.get("ray_actor_options", {})

        runtime_env = ray_actor_options.setdefault("runtime_env", {})
        
        if ENABLE_WORKER_PROCESS_SETUP_HOOK:
            runtime_env.setdefault(
                "worker_process_setup_hook",
                "ray.llm._internal.serve._worker_process_setup_hook",
            )

        if llm_config.runtime_env:
            runtime_env.update(llm_config.runtime_env)

        deployment_options["ray_actor_options"] = ray_actor_options

        return deployment_options
