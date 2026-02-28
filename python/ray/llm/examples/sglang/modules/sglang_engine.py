import copy
import signal
import time
from typing import (
    Any,
    AsyncGenerator,
    List,
    Optional,
)

from ray.llm._internal.serve.constants import ENABLE_WORKER_PROCESS_SETUP_HOOK
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingCompletionRequest,
    EmbeddingRequest,
    EmbeddingResponse,
)
from ray.llm._internal.serve.core.protocol import RawRequestInfo
from ray.llm._internal.serve.core.server.llm_server import (
    _merge_replica_actor_and_child_actor_bundles,
)


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

        # TODO(issue-61108): remove this once sglang#18752 is merged and included
        # in the minimum supported SGLang version for this example.
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

    @staticmethod
    def _build_sampling_params(request: Any) -> dict[str, Any]:
        sampling_params: dict[str, Any] = {}
        model_fields_set = getattr(request, "model_fields_set", None)
        has_model_fields_set = model_fields_set is not None
        fields_set = set(model_fields_set) if has_model_fields_set else set()

        def was_explicitly_set(field_name: str) -> bool:
            # Use model_fields_set when available to avoid injecting defaults for
            # fields omitted by the caller.
            if has_model_fields_set:
                return field_name in fields_set
            return getattr(request, field_name, None) is not None

        temperature = getattr(request, "temperature", None)
        top_p = getattr(request, "top_p", None)
        max_tokens = getattr(request, "max_tokens", None)
        stop = getattr(request, "stop", None)

        if was_explicitly_set("temperature") and temperature is not None:
            sampling_params["temperature"] = temperature
        if was_explicitly_set("top_p") and top_p is not None:
            sampling_params["top_p"] = top_p
        if was_explicitly_set("max_tokens") and max_tokens is not None:
            sampling_params["max_new_tokens"] = max_tokens
        if was_explicitly_set("stop") and stop is not None:
            sampling_params["stop"] = stop

        return sampling_params

    @staticmethod
    def _build_chat_messages(messages: List[Any]) -> List[dict[str, Any]]:
        converted_messages: List[dict[str, Any]] = []
        for message in messages:
            if isinstance(message, dict):
                message_dict = dict(message)
            elif hasattr(message, "model_dump") and callable(message.model_dump):
                message_dict = dict(message.model_dump())
            else:
                message_dict = {
                    "role": getattr(message, "role", "user"),
                    "content": getattr(message, "content", ""),
                }

            message_dict["role"] = str(message_dict.get("role", "user"))
            converted_messages.append(message_dict)
        return converted_messages

    @staticmethod
    def _build_chat_template_kwargs(request: ChatCompletionRequest) -> dict[str, Any]:
        """
        Build optional chat-template kwargs using request fields when present.
        This mirrors SGLang's chat-serving pipeline semantics without directly
        coupling to its internal server classes.
        """
        kwargs: dict[str, Any] = {}

        tools = getattr(request, "tools", None)
        if tools is not None:
            kwargs["tools"] = tools

        reasoning_effort = getattr(request, "reasoning_effort", None)
        if reasoning_effort is not None:
            kwargs["reasoning_effort"] = reasoning_effort

        chat_template_kwargs = getattr(request, "chat_template_kwargs", None)
        if isinstance(chat_template_kwargs, dict):
            kwargs.update(chat_template_kwargs)

        return kwargs

    def _render_chat_prompt(
        self,
        request: ChatCompletionRequest,
        messages: List[dict[str, Any]],
    ) -> str:
        tokenizer = self.engine.tokenizer_manager.tokenizer
        # SGLang supports --skip-tokenizer-init, where tokenizer is intentionally
        # None and text prompt rendering is not available.
        if tokenizer is None:
            return self._render_fallback_prompt(messages)

        template_kwargs = self._build_chat_template_kwargs(request)
        return tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            **template_kwargs,
        )

    @staticmethod
    def _render_fallback_prompt(messages: List[dict[str, Any]]) -> str:
        # Fallback prompt format for tokenizers without chat-template support.
        prompt_lines: List[str] = []
        for message in messages:
            role = str(message.get("role", "user"))
            content = message.get("content", "")
            if content is None:
                content = ""
            prompt_lines.append(f"{role}: {content}")
        prompt_lines.append("assistant:")
        return "\n".join(prompt_lines)

    async def start(self) -> None:
        # Engine is initialized in __init__; keep start idempotent for protocol
        # compatibility.
        return

    async def check_health(self) -> None:
        # SGLang's in-process Engine API does not expose a health-check method.
        # Its health endpoints exist only in HTTP/gRPC server entrypoints, which
        # this integration does not run. Keep the protocol hook as a no-op.
        return

    async def _generate_raw(
        self,
        request: Any,
        prompt: Any,
    ) -> dict[str, Any]:
        """Run generation and return raw engine output payload."""
        sampling_params = self._build_sampling_params(request)
        generate_kwargs = {
            "prompt": prompt,
            "stream": False,
        }
        if sampling_params:
            generate_kwargs["sampling_params"] = sampling_params
        return await self.engine.async_generate(**generate_kwargs)

    @staticmethod
    def _extract_generation_metadata(raw: dict[str, Any]) -> dict[str, Any]:
        """Extract normalized generation metadata from one raw engine payload."""
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

        return {
            "text": text.strip(),
            "id": meta.get("id", f"sglang-gen-{int(time.time())}"),
            "created": int(time.time()),
            "finish_reason": finish_reason,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    async def _generate_and_extract_metadata(
        self,
        request: Any,
        prompt: Any,
    ) -> dict[str, Any]:
        """
        Handles parameter extraction, calls the SGLang engine, and processes the
        raw response to extract common metadata and generated text.
        """
        raw = await self._generate_raw(request, prompt)
        if isinstance(raw, list):
            if not raw:
                raise RuntimeError(
                    "SGLang engine returned an empty response list during generation."
                )
            raw = raw[0]
        return self._extract_generation_metadata(raw)

    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[ChatCompletionResponse, None]:
        chat_messages = self._build_chat_messages(request.messages)
        prompt = self._render_chat_prompt(request, chat_messages)

        metadata = await self._generate_and_extract_metadata(
            request,
            prompt,
        )

        usage_data = {
            "prompt_tokens": metadata["prompt_tokens"],
            "completion_tokens": metadata["completion_tokens"],
            "total_tokens": metadata["total_tokens"],
        }

        choice_data = {
            "index": 0,
            "message": {"role": "assistant", "content": metadata["text"]},
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
        self,
        request: CompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[CompletionResponse, None]:
        prompt_input = request.prompt

        prompts_to_process: List[str] = []
        if isinstance(prompt_input, list):
            # Check for empty list
            if not prompt_input:
                raise ValueError(
                    "The 'prompt' list cannot be empty for completion requests."
                )
            # Batched prompts: process all of them
            prompts_to_process = prompt_input
        else:
            # Single string prompt: wrap it in a list for iteration
            prompts_to_process = [prompt_input]

        all_choices = []
        total_prompt_tokens = 0
        total_completion_tokens = 0
        last_metadata = {}

        # 2. Loop through all prompts in the batch
        for index, prompt_string in enumerate(prompts_to_process):
            metadata = await self._generate_and_extract_metadata(request, prompt_string)
            last_metadata = metadata  # Keep track of the metadata from the last run

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

        # Use metadata from the last generation for shared fields (id, created)
        resp = CompletionResponse(
            id=last_metadata.get("id", f"sglang-batch-gen-{int(time.time())}"),
            object="text_completion",
            created=last_metadata.get("created", int(time.time())),
            model=getattr(request, "model", "default_model"),
            choices=all_choices,
            usage=usage_data,
        )

        yield resp

    async def embeddings(
        self, request: EmbeddingRequest, raw_request: Optional[Any] = None
    ) -> AsyncGenerator[EmbeddingResponse, None]:
        # Input handling follows SGLang's OpenAIServingEmbedding pattern:
        # https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/entrypoints/openai/serving_embedding.py
        if isinstance(request, EmbeddingCompletionRequest):
            prompt = request.input
        else:
            # Chat embedding request - convert messages to prompt
            prompt = self._render_fallback_prompt(
                self._build_chat_messages(request.messages)
            )

        # async_encode handles both single strings and lists of strings
        results = await self.engine.async_encode(prompt)
        if not isinstance(results, list):
            results = [results]

        if not results:
            raise RuntimeError(
                "SGLang engine returned an empty response for embedding request."
            )

        # Build response following SGLang's _build_embedding_response pattern
        data = []
        total_prompt_tokens = 0

        for idx, ret_item in enumerate(results):
            data.append(
                {
                    "index": idx,
                    "object": "embedding",
                    "embedding": ret_item.get("embedding", []),
                }
            )
            meta = ret_item.get("meta_info", {}) or {}
            total_prompt_tokens += int(meta.get("prompt_tokens", 0))

        resp = EmbeddingResponse(
            object="list",
            model=request.model or "",
            data=data,
            usage={
                "prompt_tokens": total_prompt_tokens,
                "total_tokens": total_prompt_tokens,
                "completion_tokens": 0,
            },
        )

        yield resp

    async def llm_config(self) -> Optional[LLMConfig]:
        return self._llm_config

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        deployment_options = copy.deepcopy(llm_config.deployment_config)
        pg_config = llm_config.placement_group_config or {}

        ray_actor_options = deployment_options.get("ray_actor_options", {})

        tp_size = llm_config.engine_kwargs.get("tp_size", 1)
        pp_size = llm_config.engine_kwargs.get("pp_size", 1)
        num_devices = tp_size * pp_size

        if tp_size < 1 or pp_size < 1:
            raise ValueError(
                f"Invalid configuration: tp_size={tp_size} and pp_size={pp_size}. "
                f"Both must be >= 1."
            )

        if "placement_group_bundles" not in pg_config:
            child_bundles = [{"GPU": 1} for _ in range(num_devices)]

            replica_bundle = {
                "CPU": ray_actor_options.get("num_cpus", 1),
            }

            if ray_actor_options.get("num_gpus"):
                replica_bundle["GPU"] = ray_actor_options["num_gpus"]

            if "memory" in ray_actor_options:
                replica_bundle["memory"] = ray_actor_options["memory"]

            replica_bundle.update(ray_actor_options.get("resources", {}))

            pg_bundles = _merge_replica_actor_and_child_actor_bundles(
                child_actor_bundles=child_bundles,
                replica_actor_bundle=replica_bundle,
            )

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
