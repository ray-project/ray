import copy
import json
import signal
import time
import uuid
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    List,
    Optional,
    Union,
)

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
)


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
            content = message.get("content")
            if content is None:
                content = ""
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
    def __init__(self, llm_config: LLMConfig):

        self._llm_config = llm_config
        self.engine_kwargs = llm_config.engine_kwargs

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

    def _build_sampling_params(self, request: Any) -> dict[str, Any]:
        """Extract sampling parameters from a request."""
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

        return {
            "temperature": temp,
            "max_new_tokens": max_tokens,
            "stop": stop_sequences,
            "top_p": top_p,
        }

    @staticmethod
    def _parse_finish_reason(finish_reason_info: Any) -> str:
        """Parse finish_reason from SGLang metadata."""
        if isinstance(finish_reason_info, dict):
            return finish_reason_info.get("type", "length")
        return str(finish_reason_info)

    async def _generate_and_extract_metadata(
        self, request: Any, prompt_string: str
    ) -> dict[str, Any]:
        """
        Calls the SGLang engine in non-streaming mode and extracts
        common metadata and generated text from the response.
        """
        sampling_params = self._build_sampling_params(request)

        raw = await self.engine.async_generate(
            prompt=prompt_string,
            sampling_params=sampling_params,
            stream=False,
        )

        if isinstance(raw, list):
            if not raw:
                raise RuntimeError(
                    "SGLang engine returned an empty response list during generation."
                )
            raw = raw[0]

        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}
        finish_reason = self._parse_finish_reason(finish_reason_info)

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        return {
            "text": text.strip(),
            "id": meta.get("id", f"sglang-gen-{int(time.time())}"),
            "created": int(time.time()),
            "finish_reason": finish_reason,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    async def _stream_generate(
        self, request: Any, prompt_string: str
    ) -> AsyncGenerator[tuple[str, Optional[str]], None]:
        """Stream from SGLang engine, yielding (delta_text, finish_reason) tuples.

        SGLang returns cumulative text in each chunk, so this method
        tracks the previous text and yields only the incremental delta.
        """
        sampling_params = self._build_sampling_params(request)

        stream = await self.engine.async_generate(
            prompt=prompt_string,
            sampling_params=sampling_params,
            stream=True,
        )

        previous_text = ""
        async for chunk in stream:
            text = chunk.get("text", "")
            meta = chunk.get("meta_info", {}) or {}

            delta_text = text[len(previous_text) :]
            previous_text = text

            finish_reason_info = meta.get("finish_reason", None)
            finish_reason = (
                self._parse_finish_reason(finish_reason_info)
                if finish_reason_info is not None
                else None
            )
            yield delta_text, finish_reason

    async def chat(
        self, request: ChatCompletionRequest, raw_request: Optional[Any] = None
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse], None]:

        prompt_string = format_messages_to_prompt(request.messages)

        if request.stream:
            gen_id = f"sglang-gen-{uuid.uuid4().hex}"
            created = int(time.time())
            async for delta_text, finish_reason in self._stream_generate(
                request, prompt_string
            ):
                chunk_data = {
                    "id": gen_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": request.model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "role": ChatRole.assistant.value,
                                "content": delta_text,
                            },
                            "finish_reason": finish_reason,
                        }
                    ],
                }
                yield f"data: {json.dumps(chunk_data)}\n\n"
            return

        metadata = await self._generate_and_extract_metadata(request, prompt_string)

        usage_data = {
            "prompt_tokens": metadata["prompt_tokens"],
            "completion_tokens": metadata["completion_tokens"],
            "total_tokens": metadata["total_tokens"],
        }

        choice_data = {
            "index": 0,
            "message": {"role": ChatRole.assistant.value, "content": metadata["text"]},
            "finish_reason": metadata["finish_reason"],
        }

        resp = ChatCompletionResponse(
            id=metadata["id"],
            object="chat.completion",
            created=metadata["created"],
            model=request.model,
            choices=[choice_data],
            usage=usage_data,
        )

        yield resp

    async def completions(
        self, request: CompletionRequest, raw_request: Optional[Any] = None
    ) -> AsyncGenerator[Union[str, CompletionResponse], None]:

        prompt_input = request.prompt

        # Normalize prompt input.
        if isinstance(prompt_input, list):
            if not prompt_input:
                raise ValueError(
                    "The 'prompt' list cannot be empty for completion requests."
                )
            prompts_to_process = prompt_input
        else:
            prompts_to_process = [prompt_input]

        if request.stream:
            gen_id = f"sglang-gen-{uuid.uuid4().hex}"
            created = int(time.time())
            for i, prompt_string in enumerate(prompts_to_process):
                async for delta_text, finish_reason in self._stream_generate(
                    request, prompt_string
                ):
                    chunk_data = {
                        "id": gen_id,
                        "object": "text_completion",
                        "created": created,
                        "model": request.model,
                        "choices": [
                            {
                                "index": i,
                                "text": delta_text,
                                "logprobs": None,
                                "finish_reason": finish_reason,
                            }
                        ],
                    }
                    yield f"data: {json.dumps(chunk_data)}\n\n"
            return

        all_choices = []
        total_prompt_tokens = 0
        total_completion_tokens = 0
        last_metadata = {}

        for index, prompt_string in enumerate(prompts_to_process):
            metadata = await self._generate_and_extract_metadata(request, prompt_string)
            last_metadata = metadata

            total_prompt_tokens += metadata["prompt_tokens"]
            total_completion_tokens += metadata["completion_tokens"]

            choice_data = {
                "index": index,
                "text": metadata["text"],
                "logprobs": None,
                "finish_reason": metadata["finish_reason"],
            }
            all_choices.append(choice_data)

        usage_data = {
            "prompt_tokens": total_prompt_tokens,
            "completion_tokens": total_completion_tokens,
            "total_tokens": total_prompt_tokens + total_completion_tokens,
        }

        resp = CompletionResponse(
            id=last_metadata.get("id", f"sglang-batch-gen-{int(time.time())}"),
            object="text_completion",
            created=last_metadata.get("created", int(time.time())),
            model=getattr(request, "model", "default_model"),
            choices=all_choices,
            usage=usage_data,
        )

        yield resp

    async def llm_config(self) -> Optional[LLMConfig]:
        return self._llm_config

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        deployment_options = copy.deepcopy(llm_config.deployment_config)
        pg_config = llm_config.placement_group_config or {}

        tp_size = llm_config.engine_kwargs.get("tp_size", 1)

        if "placement_group_bundles" not in pg_config:
            pg_bundles = [{"CPU": 1, "GPU": 1}]
            if tp_size > 1:  # TO DO: to support tp_size > 1 cases
                pg_bundles.extend([{"GPU": 1} for _ in range(tp_size - 1)])
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

        # set as default without checking ENABLE_WORKER_PROCESS_SETUP_HOOK
        runtime_env.setdefault(
            "worker_process_setup_hook",
            "ray.llm._internal.serve._worker_process_setup_hook",
        )

        if llm_config.runtime_env:
            runtime_env.update(llm_config.runtime_env)

        deployment_options["ray_actor_options"] = ray_actor_options

        return deployment_options
