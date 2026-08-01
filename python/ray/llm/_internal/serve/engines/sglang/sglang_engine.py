"""SGLang engine integration for Ray Serve LLM.

Provides ``SGLangServer``, a custom server class that wraps SGLang's
in-process engine and exposes chat, completions, embeddings, tokenize,
and detokenize endpoints through the standard Ray Serve LLM protocol.

Community SGLang support is in early development. Track progress and
provide feedback at https://github.com/ray-project/ray/issues/61114.
"""

import copy
import json
import signal
import time
import uuid
from typing import (
    Any,
    AsyncGenerator,
    List,
    Literal,
    Optional,
    Union,
)

from pydantic import BaseModel

from ray.llm._internal.common.utils.cloud_utils import CloudMirrorConfig
from ray.llm._internal.serve.constants import ENABLE_WORKER_PROCESS_SETUP_HOOK
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    DetokenizeRequest,
    DetokenizeResponse,
    EmbeddingCompletionRequest,
    EmbeddingRequest,
    EmbeddingResponse,
    TokenizeCompletionRequest,
    TokenizeRequest,
    TokenizeResponse,
)
from ray.llm._internal.serve.core.protocol import RawRequestInfo
from ray.llm._internal.serve.core.server.llm_server import (
    _merge_replica_actor_and_child_actor_bundles,
)


class SGLangEngineConfig(BaseModel):
    """Minimal engine config for SGLang, exposing the fields telemetry needs.

    Unlike VLLMEngineConfig, this does not drive engine construction —
    SGLangServer.__init__ passes engine_kwargs to sglang.Engine directly.
    This class exists only to satisfy LLMConfig.get_engine_config() callers
    (e.g. usage telemetry) without requiring vLLM to be installed.
    """

    tensor_parallel_degree: int
    num_devices: int
    model_id: str
    # The HuggingFace id / local path SGLang loads from. None for a pure
    # CloudMirrorConfig source (mirror fetched under model_id); actual_hf_model_id
    # then falls back to model_id. Overwritten with the on-disk path after
    # download (see SGLangServer._download_and_resolve_model).
    hf_model_id: Optional[str] = None
    # Cloud bucket the weights are mirrored from, when model_source is a remote
    # URI or a CloudMirrorConfig. None for local / HF-hub sources.
    mirror_config: Optional["CloudMirrorConfig"] = None

    @property
    def actual_hf_model_id(self) -> str:
        return self.hf_model_id or self.model_id

    @classmethod
    def from_llm_config(cls, llm_config: "LLMConfig") -> "SGLangEngineConfig":
        from ray.llm._internal.common.utils.cloud_utils import (
            CloudMirrorConfig,
            is_remote_path,
        )

        tp_size = llm_config.engine_kwargs.get("tp_size", 1)
        pp_size = llm_config.engine_kwargs.get("pp_size", 1)

        # Mirror the vLLM mapping: resolve model_source into (hf_model_id,
        # mirror_config). A remote URI or CloudMirrorConfig is a download
        # address, not a HF id — the weights are fetched under model_id.
        hf_model_id, mirror_config = None, None
        model_source = llm_config.model_loading_config.model_source
        if model_source is None:
            hf_model_id = llm_config.model_id
        elif isinstance(model_source, str):
            if is_remote_path(model_source):
                hf_model_id = llm_config.model_id
                mirror_config = CloudMirrorConfig(bucket_uri=model_source)
            else:
                hf_model_id = model_source
        else:
            # CloudMirrorConfig (or subtype).
            mirror_config = model_source

        return cls(
            tensor_parallel_degree=tp_size,
            num_devices=tp_size * pp_size,
            model_id=llm_config.model_id,
            hf_model_id=hf_model_id,
            mirror_config=mirror_config,
        )


class SGLangPauseConfig(BaseModel):
    """SGLang-specific configuration for pause operation."""

    mode: Literal["abort", "in_place", "retract"] = "abort"
    """Pause mode:
    - "abort" (default): Terminate all in-flight requests immediately.
    - "in_place": Freeze requests in queue, preserve kv cache.
    - "retract": Freeze requests in queue, free corresponding KV cache.
    """


class SGLangSleepConfig(BaseModel):
    """SGLang-specific configuration for sleep operation"""

    tags: Optional[List[Literal["kv_cache", "weights", "cuda_graph"]]] = None

    """Sleep tags:
    - "kv_cache": Discard KV cache
    - "weights": Offload to CPU RAM
    - "cuda_graph": Discard CUDA graph
    - None: Discard/Offload everything
    """


class SGLangWakeupConfig(BaseModel):
    """SGLang-specific configuration for wakeup operation"""

    tags: Optional[List[Literal["kv_cache", "weights", "cuda_graph"]]] = None
    """Optional tags to selectively wake up components:
    - "kv_cache": Restore KV cache only
    - "weights": Restore weights only
    - "cuda_graph": Restore CUDA graph only
    - None: Restore everything
    """


_SLEEP_TAGS: frozenset[str] = frozenset({"kv_cache", "weights", "cuda_graph"})


class SGLangServer:
    def __init__(self, llm_config: LLMConfig):

        self._llm_config = llm_config
        self.engine_kwargs = llm_config.engine_kwargs
        self._is_paused = False
        self._sleeping_tags: set[str] = set()

        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e

        # Create + setup the KV-connector backend (if any) BEFORE snapshotting
        # engine_kwargs below. For SGLang PD the connector's setup() mutates
        # engine_kwargs in place (host, disaggregation_bootstrap_port) and stores
        # the backend on llm_config so the PD orchestrator can reach it. Must run
        # before sglang.Engine is constructed so those kwargs reach the engine.
        llm_config.setup_engine_backend()

        # TODO(issue-61108): remove this once sglang#18752 is merged and included
        # in the minimum supported SGLang version for this example.
        original_signal_func = signal.signal

        def noop_signal_handler(sig, action):
            # Returns default handler to satisfy signal.signal() return signature
            return signal.SIG_DFL

        # Inject model_path from model_loading_config if the user hasn't set it
        # explicitly in engine_kwargs. This mirrors what VLLMEngineConfig does for
        # vLLM — the user specifies the model via model_loading_config.model_source
        # and the engine layer resolves it to the identifier SGLang loads from,
        # preferring the on-disk copy Ray already downloaded (mirror or HF cache).
        engine_init_kwargs = dict(self.engine_kwargs)

        if "model_path" not in engine_init_kwargs:
            engine_init_kwargs["model_path"] = self._download_and_resolve_model(
                llm_config
            )

        if not engine_init_kwargs.get("model_path"):
            raise ValueError(
                "SGLang engine requires 'model_path' but it could not be determined. "
                "Set it via model_loading_config.model_source or "
                "engine_kwargs['model_path'] directly."
            )

        try:
            # Override signal.signal with our no-op function
            signal.signal = noop_signal_handler
            self.engine = sglang.Engine(**engine_init_kwargs)
        finally:
            signal.signal = original_signal_func

    @staticmethod
    def _download_and_resolve_model(llm_config: LLMConfig) -> str:
        """Download the model (if mirrored) and return the path SGLang loads from.

        SGLang builds its in-process engine here in ``__init__`` and, unlike the
        vLLM path, does not go through ``initialize_node``. So the download must
        happen on this replica's node before the engine starts:

          * ``SGLangEngineConfig.from_llm_config`` mapped ``model_source`` into
            ``actual_hf_model_id`` (the id/path) + ``mirror_config`` (the cloud
            bucket, if any), mirroring ``VLLMEngineConfig``.
          * ``download_model_files`` fetches a mirrored model under
            ``actual_hf_model_id`` and returns its local path; for a local path
            or plain HF id (no mirror_config) it returns the id unchanged.
          * ``get_model_location_on_disk`` then prefers an existing on-disk
            snapshot (mirror or HF cache) over a bare HF id.

        Result: mirrored weights load from the on-disk copy Ray fetched, never a
        HuggingFace id pointing at weights that were meant to come from the mirror.
        """
        from ray.llm._internal.common.utils.download_utils import (
            STREAMING_LOAD_FORMATS,
            NodeModelDownloadable,
            download_model_files,
            get_model_location_on_disk,
        )

        engine_config = llm_config.get_engine_config()

        # STREAMING_LOAD_FORMATS pull weights lazily at load time; don't
        # pre-download (matches the vLLM callback ctx decision).
        if llm_config.engine_kwargs.get("load_format") in STREAMING_LOAD_FORMATS:
            download_model = NodeModelDownloadable.NONE
        else:
            download_model = NodeModelDownloadable.MODEL_AND_TOKENIZER

        local_path = download_model_files(
            model_id=engine_config.actual_hf_model_id,
            mirror_config=engine_config.mirror_config,
            download_model=download_model,
            download_extra_files=True,
        )

        # download_model_files returns the local path for a mirror, else the id.
        # For a local path or plain HF id (no mirror), still prefer an existing
        # on-disk snapshot (mirror or HF cache).
        if not (local_path and local_path != engine_config.actual_hf_model_id):
            local_path = get_model_location_on_disk(engine_config.actual_hf_model_id)

        # Write the resolved path back onto the cached engine config so later
        # readers of actual_hf_model_id see the on-disk location, not the
        # pre-download id (mirrors the vLLM path in vllm_engine.py).
        if local_path and local_path != engine_config.actual_hf_model_id:
            engine_config.hf_model_id = local_path

        return local_path

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
    def _parse_finish_reason(finish_reason_info: Any) -> str:
        """Parse finish_reason from SGLang metadata."""
        if isinstance(finish_reason_info, dict):
            return finish_reason_info.get("type", "length")
        return str(finish_reason_info)

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
    def _build_chat_template_kwargs(request: Any) -> dict[str, Any]:
        """
        Build optional chat-template kwargs using request fields when present.
        This mirrors SGLang's chat-serving pipeline semantics without directly
        coupling to its internal server classes.

        Works with both ChatCompletionRequest and TokenizeChatRequest since
        both expose tools and chat_template_kwargs fields.
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
        messages: List[dict[str, Any]],
        add_generation_prompt: bool = True,
        template_kwargs: Optional[dict[str, Any]] = None,
    ) -> str:
        tokenizer = self.engine.tokenizer_manager.tokenizer
        # SGLang supports --skip-tokenizer-init, where tokenizer is intentionally
        # None and text prompt rendering is not available.
        if tokenizer is None:
            return self._render_fallback_prompt(
                messages, add_generation_prompt=add_generation_prompt
            )

        return tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=add_generation_prompt,
            **(template_kwargs or {}),
        )

    @staticmethod
    def _render_fallback_prompt(
        messages: List[dict[str, Any]],
        add_generation_prompt: bool = True,
    ) -> str:
        # Fallback prompt format for tokenizers without chat-template support.
        prompt_lines: List[str] = []
        for message in messages:
            role = str(message.get("role", "user"))
            content = message.get("content", "")
            if content is None:
                content = ""
            prompt_lines.append(f"{role}: {content}")
        if add_generation_prompt:
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

    def routing_stats(self) -> dict:
        # SGLang has no KV-events-based routing integration (unlike VLLMEngine),
        # so there is nothing to surface. Non-PD SGLang deploys this class
        # directly as server_cls and never calls this; PD wraps it in LLMServer,
        # whose record_routing_stats polls this every replica, so it must exist.
        return {}

    def _build_generate_kwargs(
        self, request: Any, prompt: Any, stream: bool
    ) -> dict[str, Any]:
        """Build kwargs dict for engine.async_generate."""
        generate_kwargs: dict[str, Any] = {
            "prompt": prompt,
            "stream": stream,
        }
        sampling_params = self._build_sampling_params(request)
        if sampling_params:
            generate_kwargs["sampling_params"] = sampling_params

        # PD disaggregation: pass bootstrap fields if present on the request.
        # These are set by the SGLang PD connector before calling the local engine
        # (decode role) or before forwarding to the prefill server (prefill role).
        bootstrap_room = getattr(request, "bootstrap_room", None)
        if bootstrap_room is not None:
            generate_kwargs["bootstrap_room"] = bootstrap_room
            bootstrap_host = getattr(request, "bootstrap_host", None)
            if bootstrap_host is not None:
                generate_kwargs["bootstrap_host"] = bootstrap_host
            bootstrap_port = getattr(request, "bootstrap_port", None)
            if bootstrap_port is not None:
                generate_kwargs["bootstrap_port"] = bootstrap_port

        return generate_kwargs

    async def _generate_raw(
        self,
        request: Any,
        prompt: Any,
    ) -> dict[str, Any]:
        """Run generation and return raw engine output payload."""
        generate_kwargs = self._build_generate_kwargs(request, prompt, stream=False)
        return await self.engine.async_generate(**generate_kwargs)

    @staticmethod
    def _extract_generation_metadata(raw: dict[str, Any]) -> dict[str, Any]:
        """Extract normalized generation metadata from one raw engine payload."""
        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}
        finish_reason = SGLangServer._parse_finish_reason(finish_reason_info)

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        return {
            "text": text.strip(),
            "id": meta.get("id", f"sglang-gen-{uuid.uuid4().hex}"),
            "created": int(time.time()),
            "finish_reason": finish_reason,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    async def _generate_and_extract_metadata(
        self,
        request: Any,
        prompt: Union[str, List[str]],
    ) -> Union[dict[str, Any], List[dict[str, Any]]]:
        """
        Handles parameter extraction, calls the SGLang engine, and processes the
        raw response to extract common metadata and generated text.

        Accepts either a single prompt string or a list of prompts. When a list
        is provided, all prompts are sent to SGLang in one batched call, letting
        SGLang's scheduler handle concurrency natively via async_generate.
        """
        raw = await self._generate_raw(request, prompt)

        # Batch case — SGLang returns a list of results, one per prompt
        if isinstance(prompt, list):
            if not raw:
                raise RuntimeError(
                    "SGLang engine returned an empty response list during generation."
                )
            return [self._extract_generation_metadata(r) for r in raw]

        # Single prompt case
        if isinstance(raw, list):
            if not raw:
                raise RuntimeError(
                    "SGLang engine returned an empty response list during generation."
                )
            raw = raw[0]
        return self._extract_generation_metadata(raw)

    async def _stream_generate(
        self,
        request: Any,
        prompt: Any,
    ) -> AsyncGenerator[tuple[str, Optional[str]], None]:
        """Stream from SGLang engine, yielding (delta_text, finish_reason) tuples.

        SGLang returns cumulative text in each chunk, so this method
        tracks the previous text and yields only the incremental delta.
        """
        generate_kwargs = self._build_generate_kwargs(request, prompt, stream=True)
        stream = await self.engine.async_generate(**generate_kwargs)

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

    @staticmethod
    def _build_sse_chunk(
        gen_id: str,
        object_type: str,
        created: int,
        model: str,
        choice: dict[str, Any],
    ) -> str:
        """Build an SSE-formatted chunk string from a single choice payload."""
        chunk_data = {
            "id": gen_id,
            "object": object_type,
            "created": created,
            "model": model,
            "choices": [choice],
        }
        return f"data: {json.dumps(chunk_data)}\n\n"

    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse], None]:
        chat_messages = self._build_chat_messages(request.messages)
        template_kwargs = self._build_chat_template_kwargs(request)
        prompt = self._render_chat_prompt(
            chat_messages, template_kwargs=template_kwargs
        )

        if request.stream:
            gen_id = f"sglang-gen-{uuid.uuid4().hex}"
            created = int(time.time())
            first_chunk = True
            async for delta_text, finish_reason in self._stream_generate(
                request, prompt
            ):
                delta: dict[str, Any] = {"content": delta_text}
                if first_chunk:
                    delta["role"] = "assistant"
                    first_chunk = False
                yield self._build_sse_chunk(
                    gen_id,
                    "chat.completion.chunk",
                    created,
                    request.model,
                    {"index": 0, "delta": delta, "finish_reason": finish_reason},
                )
            return

        metadata = await self._generate_and_extract_metadata(request, prompt)

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
                    yield self._build_sse_chunk(
                        gen_id,
                        "text_completion",
                        created,
                        request.model,
                        {
                            "index": i,
                            "text": delta_text,
                            "logprobs": None,
                            "finish_reason": finish_reason,
                        },
                    )
            return

        results = await self._generate_and_extract_metadata(request, prompts_to_process)

        all_choices = []
        total_prompt_tokens = 0
        total_completion_tokens = 0

        for index, metadata in enumerate(results):
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

        last_metadata = results[-1]

        resp = CompletionResponse(
            id=last_metadata["id"],
            object="text_completion",
            created=last_metadata.get("created", int(time.time())),
            model=getattr(request, "model", "default_model"),
            choices=all_choices,
            usage=usage_data,
        )

        yield resp

    async def embeddings(
        self,
        request: EmbeddingRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[EmbeddingResponse, None]:
        # Input handling follows SGLang's OpenAIServingEmbedding pattern:
        # https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/entrypoints/openai/serving_embedding.py
        if isinstance(request, EmbeddingCompletionRequest):
            prompt = request.input
        else:
            # Chat embedding request - join messages without the trailing
            # "assistant:" generation cue that _render_fallback_prompt adds.
            chat_messages = self._build_chat_messages(request.messages)
            prompt = "\n".join(
                f"{m.get('role', 'user')}: {m.get('content') or ''}"
                for m in chat_messages
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

    async def tokenize(
        self,
        request: TokenizeRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[TokenizeResponse, None]:
        tokenizer = self.engine.tokenizer_manager.tokenizer
        if tokenizer is None:
            raise RuntimeError(
                "Tokenizer is not available. The tokenize endpoint is not "
                "supported when SGLang is initialized with --skip-tokenizer-init."
            )

        if isinstance(request, TokenizeCompletionRequest):
            prompt = request.prompt
        else:
            # Chat tokenize request - render messages to prompt string
            chat_messages = self._build_chat_messages(request.messages)
            add_generation_prompt = getattr(request, "add_generation_prompt", True)
            template_kwargs = self._build_chat_template_kwargs(request)
            prompt = self._render_chat_prompt(
                chat_messages,
                add_generation_prompt=add_generation_prompt,
                template_kwargs=template_kwargs,
            )

        add_special_tokens = getattr(request, "add_special_tokens", True)
        tokens = tokenizer.encode(prompt, add_special_tokens=add_special_tokens)

        max_model_len = (
            getattr(self.engine.tokenizer_manager, "context_len", None)
            or getattr(self.engine.server_args, "context_length", None)
            or 0
        )

        yield TokenizeResponse(
            tokens=tokens,
            count=len(tokens),
            max_model_len=max_model_len,
        )

    async def detokenize(
        self,
        request: DetokenizeRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[DetokenizeResponse, None]:
        tokenizer = self.engine.tokenizer_manager.tokenizer
        if tokenizer is None:
            raise RuntimeError(
                "Tokenizer is not available. The detokenize endpoint is not "
                "supported when SGLang is initialized with --skip-tokenizer-init."
            )
        prompt = tokenizer.decode(request.tokens)

        yield DetokenizeResponse(text=prompt)

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

            replica_bundle.update(ray_actor_options.get("resources", {}))

            if "memory" in ray_actor_options:
                replica_bundle["memory"] = ray_actor_options["memory"]

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

    async def pause(self, **kwargs: Any) -> None:
        """Pause generation on the SGlang server

        This halts generation/encoding requests while keeping model weights in GPU memory. New requests are blocked until resume is called.

        Args:
            **kwargs: Options parsed into SGLangPauseConfig.
                - mode (str): "abort" (default), "in_place", or "retract"
        """

        assert self.engine is not None, "server is not initialized"
        config = SGLangPauseConfig(**kwargs)
        from sglang.srt.managers.io_struct import PauseGenerationReqInput

        await self.engine.tokenizer_manager.pause_generation(
            PauseGenerationReqInput(mode=config.mode)
        )
        self._is_paused = True

    async def resume(self, **kwargs: Any) -> None:
        """Resume generation on the SGLang server after pause.

        Args:
            **kwargs: Reserved for future options.
        """
        assert self.engine is not None, "server is not initialized"
        from sglang.srt.managers.io_struct import ContinueGenerationReqInput

        await self.engine.tokenizer_manager.continue_generation(
            ContinueGenerationReqInput()
        )
        self._is_paused = False

    async def is_paused(self) -> bool:
        """Check whether the SGLang server is currently paused.

        Returns:
            True if the server is paused, False otherwise.
        """
        return self._is_paused

    async def sleep(self, **kwargs: Any) -> None:
        """Put SGLang server to sleep.

        Args:
            **kwargs: Options parsed into SGLangSleepConfig
                - tags (List[str], optional): Components to put to sleep.
        """

        assert self.engine is not None, "server is not initialized"
        config = SGLangSleepConfig(**kwargs)

        # release_memory_occupation() calls loop.run_until_complete() internally, which fails
        # inside an async context. Await the underlying coroutine directly.
        from sglang.srt.entrypoints.engine import ReleaseMemoryOccupationReqInput

        obj = ReleaseMemoryOccupationReqInput(tags=config.tags)
        await self.engine.tokenizer_manager.release_memory_occupation(obj, None)
        self._sleeping_tags |= set(config.tags) if config.tags else set(_SLEEP_TAGS)

    async def wakeup(self, **kwargs: Any) -> None:
        """Wake up the SGLang server from sleep mode.

        Args:
            **kwargs: Options parsed into SGLangWakeupConfig
                - tags (List[str], optional): Components to wake up.
        """

        assert self.engine is not None, "server is not initialized"
        config = SGLangWakeupConfig(**kwargs)
        # resume_memory_occupation() release_memory_occupation() calls loop.run_until_complete() internally, which fails
        # inside an async context. Await the underlying coroutine directly.
        from sglang.srt.entrypoints.engine import ResumeMemoryOccupationReqInput

        obj = ResumeMemoryOccupationReqInput(tags=config.tags)
        await self.engine.tokenizer_manager.resume_memory_occupation(obj, None)

        if config.tags is None:
            self._sleeping_tags.clear()
        else:
            self._sleeping_tags -= set(config.tags)

    async def is_sleeping(self) -> bool:
        """Check whether the SGLang server is currently sleeping.

        Returns:
            True if any component is currently offloaded/discarded, False otherwise.
        """
        return bool(self._sleeping_tags)

    async def reset_prefix_cache(self, timeout: Optional[float] = None) -> None:
        assert self.engine is not None, "server is not initialized"
        # flush_cache() calls loop.run_until_complete() internally, which fails
        # inside an async context. Await the underlying coroutine directly.
        await self.engine.tokenizer_manager.flush_cache()
