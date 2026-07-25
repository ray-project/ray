import argparse
import asyncio
from collections import OrderedDict
import dataclasses
import functools
import inspect
import json
import os
import time
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
import zlib

from fastapi import HTTPException as FastAPIHTTPException
from fastapi import Response
import numpy as np
from pydantic import BaseModel, field_validator
from starlette.datastructures import State
from starlette.requests import Request
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.entrypoints.openai.cli_args import FrontendArgs
from vllm.entrypoints.openai.engine.protocol import ErrorResponse as VLLMErrorResponse

import ray
from ray.llm._internal.common.callbacks.base import CallbackCtx
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.core.configs.llm_config import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    DetokenizeRequest,
    DetokenizeResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorInfo,
    ErrorResponse,
    ScoreRequest,
    ScoreResponse,
    TokenizeRequest,
    TokenizeResponse,
    TranscriptionRequest,
    TranscriptionResponse,
)
from ray.llm._internal.serve.core.engine.protocol import LLMEngine
from ray.llm._internal.serve.core.protocol import RawRequestInfo
from ray.llm._internal.serve.engines.vllm.vllm_models import (
    VLLMEngineConfig,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_PROMPT_TOKEN_CRC32_HEADER,
    KV_PROMPT_TOKEN_KEY_HEADER,
    KV_PROMPT_TOKEN_LEN_HEADER,
    KV_PROMPT_TOKEN_SIDE_CHANNEL_PATH,
    KV_SELECT_RESERVE_KEY,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    assign_replica_kv_events_endpoint,
    get_kv_event_routing_stats,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.token_tracking import (
    enable_token_tracking,
)
from ray.llm._internal.serve.utils.node_initialization_utils import (
    initialize_node,
)
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

if TYPE_CHECKING:
    from vllm.config import VllmConfig
    from vllm.engine.protocol import EngineClient
    from vllm.entrypoints.openai.chat_completion.serving import OpenAIServingChat
    from vllm.entrypoints.openai.completion.serving import OpenAIServingCompletion
    from vllm.entrypoints.openai.models.serving import OpenAIServingModels
    from vllm.entrypoints.pooling.embed.serving import ServingEmbedding
    from vllm.entrypoints.pooling.scoring.serving import ServingScores
    from vllm.entrypoints.serve.tokenize.serving import ServingTokenization
    from vllm.entrypoints.speech_to_text.transcription.serving import (
        OpenAIServingTranscription,
    )

vllm = try_import("vllm")
logger = get_logger(__name__)

_PROMPT_TOKEN_SIDE_CHANNEL_TTL_S = float(
    os.environ.get("RAY_SERVE_KV_PROMPT_TOKEN_STAGING_TTL_S", "60")
)
_PROMPT_TOKEN_SIDE_CHANNEL_MAX_ENTRIES = int(
    os.environ.get("RAY_SERVE_KV_PROMPT_TOKEN_STAGING_MAX_ENTRIES", "8192")
)
_PROMPT_TOKEN_SIDE_CHANNEL_MAX_BYTES = int(
    os.environ.get("RAY_SERVE_KV_PROMPT_TOKEN_STAGING_MAX_BYTES", str(1024**3))
)
_PROMPT_TOKEN_SIDE_CHANNEL_WAIT_S = float(
    os.environ.get("RAY_SERVE_KV_PROMPT_TOKEN_WAIT_TIMEOUT_S", "0.25")
)
_PROMPT_TOKEN_SIDE_CHANNEL_CONSUMED = 0
_PROMPT_TOKEN_SIDE_CHANNEL_MISSED = 0
_PROMPT_TOKEN_SIDE_CHANNEL_LOGGED = False


@dataclasses.dataclass
class _StagedPromptTokens:
    payload: bytes
    token_count: int
    crc32: str
    created_at_s: float


class _PromptTokenSideChannelStore:
    """Replica-local staging area for binary prompt token vectors."""

    def __init__(
        self,
        *,
        ttl_s: float = _PROMPT_TOKEN_SIDE_CHANNEL_TTL_S,
        max_entries: int = _PROMPT_TOKEN_SIDE_CHANNEL_MAX_ENTRIES,
        max_bytes: int = _PROMPT_TOKEN_SIDE_CHANNEL_MAX_BYTES,
    ):
        self._ttl_s = ttl_s
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._entries: "OrderedDict[str, _StagedPromptTokens]" = OrderedDict()
        self._total_bytes = 0
        self._lock = asyncio.Lock()

    async def put(
        self,
        key: str,
        *,
        payload: bytes,
        token_count: int,
        crc32: str,
    ) -> None:
        if self._max_bytes > 0 and len(payload) > self._max_bytes:
            raise ValueError(
                "prompt token payload exceeds staging byte cap "
                f"({len(payload)} > {self._max_bytes})"
            )

        now = time.monotonic()
        async with self._lock:
            self._sweep_locked(now)
            old = self._entries.pop(key, None)
            if old is not None:
                self._total_bytes -= len(old.payload)
            entry = _StagedPromptTokens(
                payload=payload,
                token_count=token_count,
                crc32=crc32,
                created_at_s=now,
            )
            self._entries[key] = entry
            self._total_bytes += len(payload)
            self._evict_to_limits_locked()

    async def pop(
        self, key: str, *, wait_s: float = 0.0
    ) -> Optional[_StagedPromptTokens]:
        deadline = time.monotonic() + max(0.0, wait_s)
        while True:
            now = time.monotonic()
            async with self._lock:
                self._sweep_locked(now)
                entry = self._entries.pop(key, None)
                if entry is not None:
                    self._total_bytes -= len(entry.payload)
                    return entry
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(0.001, remaining))

    def _sweep_locked(self, now: float) -> None:
        if self._ttl_s <= 0:
            return
        while self._entries:
            _, entry = next(iter(self._entries.items()))
            if now - entry.created_at_s <= self._ttl_s:
                return
            _, expired = self._entries.popitem(last=False)
            self._total_bytes -= len(expired.payload)

    def _evict_to_limits_locked(self) -> None:
        while self._max_entries > 0 and len(self._entries) > self._max_entries:
            _, evicted = self._entries.popitem(last=False)
            self._total_bytes -= len(evicted.payload)
        while self._max_bytes > 0 and self._total_bytes > self._max_bytes:
            _, evicted = self._entries.popitem(last=False)
            self._total_bytes -= len(evicted.payload)


def _parse_required_int_header(headers: Any, name: str) -> int:
    value = headers.get(name)
    if value is None:
        raise FastAPIHTTPException(status_code=400, detail=f"missing {name}")
    try:
        parsed = int(value)
    except ValueError:
        raise FastAPIHTTPException(status_code=400, detail=f"invalid {name}") from None
    if parsed < 0:
        raise FastAPIHTTPException(status_code=400, detail=f"invalid {name}")
    return parsed


def _parse_required_crc32_header(headers: Any) -> str:
    value = headers.get(KV_PROMPT_TOKEN_CRC32_HEADER)
    if value is None:
        raise FastAPIHTTPException(
            status_code=400, detail=f"missing {KV_PROMPT_TOKEN_CRC32_HEADER}"
        )
    value = value.lower()
    if len(value) != 8 or any(ch not in "0123456789abcdef" for ch in value):
        raise FastAPIHTTPException(
            status_code=400, detail=f"invalid {KV_PROMPT_TOKEN_CRC32_HEADER}"
        )
    return value


def _prompt_token_ids_from_entry(entry: _StagedPromptTokens):
    expected_bytes = entry.token_count * 4
    if len(entry.payload) != expected_bytes:
        raise ValueError(
            "prompt token payload byte length mismatch "
            f"({len(entry.payload)} != {expected_bytes})"
        )
    crc32 = f"{zlib.crc32(entry.payload) & 0xFFFFFFFF:08x}"
    if crc32 != entry.crc32:
        raise ValueError(
            f"prompt token payload checksum mismatch ({crc32} != {entry.crc32})"
        )
    return np.frombuffer(entry.payload, dtype="<u4").tolist()


def _write_prompt_token_side_channel_marker(kind: str, value: int) -> None:
    try:
        with open(f"/tmp/kv_prompt_sidechannel_{kind}_{os.getpid()}", "w") as f:
            f.write(str(value))
    except OSError:
        pass


def _note_prompt_token_side_channel_consumed() -> None:
    global _PROMPT_TOKEN_SIDE_CHANNEL_CONSUMED, _PROMPT_TOKEN_SIDE_CHANNEL_LOGGED
    _PROMPT_TOKEN_SIDE_CHANNEL_CONSUMED += 1
    _write_prompt_token_side_channel_marker(
        "consumed", _PROMPT_TOKEN_SIDE_CHANNEL_CONSUMED
    )
    if not _PROMPT_TOKEN_SIDE_CHANNEL_LOGGED:
        _PROMPT_TOKEN_SIDE_CHANNEL_LOGGED = True
        logger.info(
            "KV prompt token side-channel consumed staged prompt IDs; "
            "vLLM renderer will skip tokenization."
        )


def _note_prompt_token_side_channel_missed(token_key: str) -> None:
    global _PROMPT_TOKEN_SIDE_CHANNEL_MISSED
    _PROMPT_TOKEN_SIDE_CHANNEL_MISSED += 1
    _write_prompt_token_side_channel_marker("missed", _PROMPT_TOKEN_SIDE_CHANNEL_MISSED)
    logger.warning(
        "KV prompt token side-channel key %s was not found on this replica; "
        "falling back to vLLM tokenization.",
        token_key,
    )


async def _inject_prompt_ids_from_side_channel(
    request: Any,
    raw_request: Optional[Request],
    store: _PromptTokenSideChannelStore,
) -> None:
    if raw_request is None:
        return
    token_key = raw_request.headers.get(KV_PROMPT_TOKEN_KEY_HEADER)
    if not token_key:
        return

    entry = await store.pop(token_key, wait_s=_PROMPT_TOKEN_SIDE_CHANNEL_WAIT_S)
    if entry is None:
        _note_prompt_token_side_channel_missed(token_key)
        return

    header_count = _parse_required_int_header(
        raw_request.headers, KV_PROMPT_TOKEN_LEN_HEADER
    )
    header_crc32 = _parse_required_crc32_header(raw_request.headers)
    if header_count != entry.token_count:
        raise ValueError(
            "prompt token side-channel token count mismatch "
            f"({header_count} != {entry.token_count})"
        )
    if header_crc32 != entry.crc32:
        raise ValueError(
            "prompt token side-channel checksum metadata mismatch "
            f"({header_crc32} != {entry.crc32})"
        )

    prompt_token_ids = _prompt_token_ids_from_entry(entry)
    kv_transfer_params = getattr(request, "kv_transfer_params", None)
    if not isinstance(kv_transfer_params, dict):
        kv_transfer_params = {}
        request.kv_transfer_params = kv_transfer_params
    kv_transfer_params["prompt_token_ids"] = prompt_token_ids
    _note_prompt_token_side_channel_consumed()


def _install_prompt_ids_forwarding(
    serving: Any,
    method_name: str,
    store: _PromptTokenSideChannelStore,
) -> None:
    if serving is None:
        return
    orig = getattr(serving, method_name, None)
    if orig is None or getattr(orig, "_kv_prompt_side_channel_wrapped", False):
        return

    @functools.wraps(orig)
    async def wrapped(request: Any, raw_request: Optional[Request] = None, *args, **kw):
        effective_raw_request = kw.get("raw_request", raw_request)
        await _inject_prompt_ids_from_side_channel(
            request, effective_raw_request, store
        )
        if "raw_request" in kw and raw_request is None:
            return await orig(request, *args, **kw)
        return await orig(request, raw_request, *args, **kw)

    wrapped._kv_prompt_side_channel_wrapped = True  # type: ignore[attr-defined]
    setattr(serving, method_name, wrapped)


def _install_prompt_token_side_channel(
    app: Any,
    store: _PromptTokenSideChannelStore,
) -> None:
    if getattr(app.state, "_kv_prompt_token_side_channel_installed", False):
        return
    app.state.kv_prompt_token_side_channel_store = store

    @app.put(KV_PROMPT_TOKEN_SIDE_CHANNEL_PATH + "/{token_key}")
    async def _stage_prompt_token_ids(token_key: str, request: Request):
        token_count = _parse_required_int_header(
            request.headers, KV_PROMPT_TOKEN_LEN_HEADER
        )
        crc32 = _parse_required_crc32_header(request.headers)
        payload = await request.body()
        if len(payload) != token_count * 4:
            raise FastAPIHTTPException(
                status_code=400,
                detail=(
                    "prompt token payload byte length mismatch "
                    f"({len(payload)} != {token_count * 4})"
                ),
            )
        actual_crc32 = f"{zlib.crc32(payload) & 0xFFFFFFFF:08x}"
        if actual_crc32 != crc32:
            raise FastAPIHTTPException(
                status_code=400,
                detail=(
                    "prompt token payload checksum mismatch "
                    f"({actual_crc32} != {crc32})"
                ),
            )
        try:
            await store.put(
                token_key,
                payload=payload,
                token_count=token_count,
                crc32=crc32,
            )
        except ValueError as e:
            raise FastAPIHTTPException(status_code=413, detail=str(e)) from e
        return Response(status_code=204)

    app.state._kv_prompt_token_side_channel_installed = True


def _install_prompt_ids_forwarding_on_state(
    state: Any,
    store: _PromptTokenSideChannelStore,
) -> None:
    _install_prompt_ids_forwarding(
        getattr(state, "openai_serving_chat", None),
        "create_chat_completion",
        store,
    )
    _install_prompt_ids_forwarding(
        getattr(state, "openai_serving_completion", None),
        "create_completion",
        store,
    )


def _canonicalize_request_id_header(
    request: Any, raw_request_info: Optional[RawRequestInfo]
) -> Optional[RawRequestInfo]:
    """Ensure raw_request_info carries X-Request-Id == request.request_id so vLLM's
    OpenAI layer (which prefers the header) sees the authoritative request id.

    Returns a RawRequestInfo (new or mutated) with a single, correctly-cased header.
    If the request has no request_id, this is a no-op and returns raw_request_info
    unchanged (so embeddings/score/transcription requests are unaffected).
    """
    rid = getattr(request, "request_id", None)
    if not rid:
        return raw_request_info
    headers = dict(raw_request_info.headers) if raw_request_info is not None else {}
    # Drop any existing variant of the header (case- and separator-insensitive,
    # e.g. "X-Request-Id" or "x_request_id") before setting the canonical one.
    headers = {
        k: v
        for k, v in headers.items()
        if k.replace("_", "-").lower() != "x-request-id"
    }
    headers["x-request-id"] = str(rid)
    if raw_request_info is None:
        return RawRequestInfo(headers=headers)
    # Preserve any non-header fields RawRequestInfo carries (now or in the future).
    return dataclasses.replace(raw_request_info, headers=headers)


def _convert_config_dicts(merged: dict) -> dict:
    """Convert dict values to their proper vLLM config classes based on type hints.

    vLLM's AsyncEngineArgs has fields like structured_outputs_config,
    compilation_config, etc. that expect dataclass instances. When users pass
    dicts for these fields, we need to convert them to the proper config classes
    so that default values are populated correctly.

    Without this conversion, dicts get converted to argparse.Namespace objects
    which lack the default field values, causing AttributeError when vLLM code
    tries to access those fields.
    """
    fields_by_name = {f.name: f for f in dataclasses.fields(AsyncEngineArgs)}

    for key, value in list(merged.items()):
        if not isinstance(value, dict) or key not in fields_by_name:
            continue

        hint = fields_by_name[key].type
        if isinstance(hint, str):
            continue

        # Handle Optional[X] (Union[X, None]) -> X
        origin = typing.get_origin(hint)
        if origin is Union:
            args = typing.get_args(hint)
            hint = next((a for a in args if a is not type(None)), hint)

        # Convert dict to dataclass if the field expects a dataclass type
        if isinstance(hint, type) and dataclasses.is_dataclass(hint):
            try:
                merged[key] = hint(**value)
            except Exception as e:
                logger.warning(
                    f"Failed to convert {key} dict to {hint.__name__}: {e}. "
                    "Using dict as-is."
                )

    return merged


def _dict_to_namespace(obj: Any) -> Any:
    """Recursively converts dictionaries to argparse.Namespace."""
    if isinstance(obj, dict):
        return argparse.Namespace(**{k: _dict_to_namespace(v) for k, v in obj.items()})
    elif isinstance(obj, list):
        return [_dict_to_namespace(item) for item in obj]
    else:
        return obj


def _get_vllm_engine_config(
    llm_config: LLMConfig,
) -> Tuple["AsyncEngineArgs", "VllmConfig"]:
    engine_config = llm_config.get_engine_config()

    # Resolve to local cache path if model was downloaded from S3/GCS mirror
    # Only do this if mirror_config was specified (intentional S3/GCS download)
    if engine_config.mirror_config:
        from ray.llm._internal.common.utils.download_utils import (
            get_model_location_on_disk,
        )

        local_path = get_model_location_on_disk(engine_config.actual_hf_model_id)
        if local_path and local_path != engine_config.actual_hf_model_id:
            engine_config.hf_model_id = local_path
            logger.info(f"Resolved model from mirror to local path: {local_path}")

    from vllm.usage.usage_lib import UsageContext

    try:
        async_engine_args = vllm.engine.arg_utils.AsyncEngineArgs(
            **engine_config.get_initialization_kwargs()
        )
        vllm_engine_config = async_engine_args.create_engine_config(
            usage_context=UsageContext.OPENAI_API_SERVER
        )
    except Exception as e:
        # vLLM's ModelConfig is a pydantic dataclass; its ValidationError holds an
        # unpicklable ArgsKwargs and cannot cross the Ray task boundary. Re-raise as
        # a plain error so the real message propagates instead of a pickling failure.
        raise RuntimeError(f"Failed to create vLLM engine config: {e}") from None
    return async_engine_args, vllm_engine_config


def _clear_current_platform_cache():
    """Clear the cache of the current platform.

    vllm current has an lru cache for getting device compatibility
    that will not have the correct returned value if
    CUDA_VISIBLE_DEVICES is not set properly. In RayLLM eventually
    when we want to create the engine the env will be set properly,
    but till then, upon the import of vllm somewhere
    (which is a mystery) the lru cache will have the wrong value.
    This function will clear the cache so that the next time the
    cache is accessed, it will be re-evaluated.

    Related issues:
    https://github.com/vllm-project/vllm/issues/8402
    https://github.com/vllm-project/vllm/issues/7890
    """
    from vllm.platforms import current_platform

    # This check is just to future proof this implementation
    # in case vllm removes their lru_cache decorator
    if hasattr(current_platform.get_device_capability, "cache_clear"):
        logger.info("Clearing the current platform cache ...")
        current_platform.get_device_capability.cache_clear()


class VLLMSleepConfig(BaseModel):
    """vLLM-specific configuration for sleep operation."""

    level: int = 1
    """Sleep level:
    - Level 1: Offload weights to CPU RAM, discard KV cache
    - Level 2: Discard both model weights and KV cache (deeper sleep)
    """

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: Any) -> int:
        if v not in (1, 2):
            raise ValueError("level must be 1 or 2")
        return v


class VLLMWakeupConfig(BaseModel):
    """vLLM-specific configuration for wakeup operation."""

    tags: Optional[List[str]] = None
    """Optional tags to selectively wake up components:
    - "weights": Restore model weights only
    - "kv_cache": Restore KV cache only
    - None: Restore everything
    """

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: Any) -> Optional[List[str]]:
        if v is not None:
            valid_tags = {"weights", "kv_cache"}
            for tag in v:
                if tag not in valid_tags:
                    raise ValueError(
                        f"Invalid tag '{tag}'. Must be one of: {valid_tags}"
                    )
        return v


class VLLMPauseConfig(BaseModel):
    """vLLM-specific configuration for pause operation."""

    mode: Literal["abort", "wait", "keep"] = "abort"
    """Pause mode:
    - "abort" (default): Abort all in-flight requests immediately.
    - "wait": Wait for in-flight requests to complete before pausing.
    - "keep": Freeze requests in queue; they resume on resume_generation().
    """

    clear_cache: bool = True
    """Whether to clear KV and prefix caches after draining.
    Set to False to preserve cache for faster resume.
    """


class VLLMEngine(LLMEngine):
    def __init__(
        self,
        llm_config: LLMConfig,
    ):
        """Create a vLLM Engine class

        Args:
            llm_config: The llm configuration for this engine
        """
        super().__init__(llm_config)

        self.llm_config = llm_config

        if vllm is None:
            raise ImportError(
                "vLLM is not installed. Please install it with `pip install ray[llm]`."
            )
        assign_replica_kv_events_endpoint(self.llm_config)
        self.llm_config.setup_engine_backend()

        self._running = False
        # Routing stats advertised to Serve's request router; populated in
        # start() once the engine's KV-events endpoint is bound.
        self._routing_stats: Dict[str, Any] = {}
        self._prompt_token_side_channel_store = _PromptTokenSideChannelStore()

        # vLLM Integration points. Will be set through .start()
        self._engine_client = None
        self._oai_models: Optional["OpenAIServingModels"] = None
        self._oai_serving_chat: Optional["OpenAIServingChat"] = None
        self._oai_serving_completion: Optional["OpenAIServingCompletion"] = None
        self._oai_serving_embedding: Optional["ServingEmbedding"] = None
        self._oai_serving_transcription: Optional["OpenAIServingTranscription"] = None
        self._oai_serving_scores: Optional["ServingScores"] = None
        self._oai_serving_tokenization: Optional["ServingTokenization"] = None

    async def build_asgi_app(self):
        from vllm.entrypoints.openai.api_server import build_app, init_app_state

        supported_tasks = ("generate",)
        if hasattr(self._engine_client, "get_supported_tasks"):
            supported_tasks = await self._engine_client.get_supported_tasks()

        # Pass model_config so vLLM mounts the pooling routers (/pooling, /classify,
        # /embed, /score) on the native ASGI app to enable direct streaming for pooling
        # classify, embed, and score.
        app = build_app(
            self._vllm_args,
            supported_tasks=supported_tasks,
            model_config=self._engine_client.model_config,
        )
        await init_app_state(
            self._engine_client,
            app.state,
            self._vllm_args,
            supported_tasks=supported_tasks,
        )
        _install_prompt_token_side_channel(
            app, self._prompt_token_side_channel_store
        )
        _install_prompt_ids_forwarding_on_state(
            app.state, self._prompt_token_side_channel_store
        )
        return app

    async def start(self) -> None:
        """Start the vLLM engine.

        If the engine is already running, do nothing.
        """

        if self._running:
            # The engine is already running!
            logger.info("Skipping engine restart because the engine is already running")
            return

        from vllm.entrypoints.openai.api_server import init_app_state

        callback = self.llm_config.get_or_create_callback()
        await callback.run_callback("on_before_node_init")
        if callback.ctx.run_init_node:
            await initialize_node(self.llm_config)
        await callback.run_callback("on_after_node_init")

        (
            vllm_engine_args,
            vllm_frontend_args,
            vllm_engine_config,
        ) = self._prepare_engine_config(callback.ctx)

        # Apply checkpoint info to the llm_config.
        # This is needed for capturing model capabilities
        # (e.g. supports vision, etc.) on the llm_config.
        config = self.llm_config.get_engine_config()
        self.llm_config.apply_checkpoint_info(
            vllm_engine_config.model_config.model,
            trust_remote_code=config.trust_remote_code,
        )

        self._engine_client = self._start_async_llm_engine(
            vllm_engine_args,
            vllm_engine_config,
            callback.ctx.placement_group,
        )

        state = State()
        # TODO (Kourosh): There might be some variables that needs protection?
        merged = vllm_frontend_args.__dict__ | vllm_engine_args.__dict__

        # Convert dict values to proper vLLM config classes (e.g., StructuredOutputsConfig)
        # so that default field values are populated correctly.
        merged = _convert_config_dicts(merged)

        args = _dict_to_namespace(merged)
        self._vllm_args = args

        # Query supported tasks from the engine so init_app_state initializes the correct serving objects.
        # Without this, vLLM falls back to 'generate' only.
        init_kwargs: dict[str, Any] = dict(
            state=state,
            args=args,
        )
        if "supported_tasks" in inspect.signature(init_app_state).parameters:
            if hasattr(self._engine_client, "get_supported_tasks"):
                supported_tasks = await self._engine_client.get_supported_tasks()
                init_kwargs["supported_tasks"] = supported_tasks

        if "vllm_config" in inspect.signature(init_app_state).parameters:
            init_kwargs["vllm_config"] = vllm_engine_config

        await init_app_state(self._engine_client, **init_kwargs)

        self._oai_models = getattr(state, "openai_serving_models", None)
        self._oai_serving_chat = getattr(state, "openai_serving_chat", None)
        self._oai_serving_completion = getattr(state, "openai_serving_completion", None)
        self._oai_serving_embedding = getattr(state, "serving_embedding", None)
        self._oai_serving_transcription = getattr(
            state, "openai_serving_transcription", None
        )
        self._oai_serving_scores = getattr(state, "serving_scores", None)
        self._oai_serving_tokenization = getattr(
            state, "openai_serving_tokenization", None
        )
        _install_prompt_ids_forwarding_on_state(
            state, self._prompt_token_side_channel_store
        )

        self._validate_openai_serving_models()
        self._validate_engine_client()

        self._routing_stats = get_kv_event_routing_stats(
            self.llm_config,
            vllm_engine_config.cache_config.block_size,
            vllm_engine_config.scheduler_config.max_num_batched_tokens,
        )

        self._running = True

        logger.info("Started vLLM engine.")

    def routing_stats(self) -> Dict[str, Any]:
        """Returns KV event and replay endpoints for KV-aware routing."""
        return self._routing_stats

    def _validate_openai_serving_models(self):
        assert self._oai_models is not None, "oai_models is not initialized"
        assert hasattr(
            self._oai_models, "lora_requests"
        ), "oai_models must have a lora_requests attribute"
        assert hasattr(
            self._oai_models, "load_lora_adapter"
        ), "oai_models must have a load_lora_adapter attribute"

    @staticmethod
    def _make_error(message: str) -> ErrorResponse:
        return ErrorResponse(
            error=ErrorInfo(message=message, type="invalid_request_error", code=400)
        )

    def _validate_openai_serving_chat(self) -> Optional[ErrorResponse]:
        if self._oai_serving_chat is None:
            return self._make_error(
                "This model does not support the 'generate' task. "
                "The chat completion endpoint is not available for this model."
            )

    def _validate_openai_serving_completion(self) -> Optional[ErrorResponse]:
        if self._oai_serving_completion is None:
            return self._make_error(
                "This model does not support the 'generate' task. "
                "The completion endpoint is not available for this model."
            )

    def _validate_openai_serving_embedding(self) -> Optional[ErrorResponse]:
        if self._oai_serving_embedding is None:
            return self._make_error(
                "This model does not support the 'embed' task. "
                "The embedding endpoint is not available for this model."
            )

    def _validate_openai_serving_transcription(self) -> Optional[ErrorResponse]:
        if self._oai_serving_transcription is None:
            return self._make_error(
                "This model does not support the 'transcription' task. "
                "The transcription endpoint is not available for this model."
            )

    def _validate_openai_serving_scores(self) -> Optional[ErrorResponse]:
        if self._oai_serving_scores is None:
            return self._make_error(
                "This model does not support the 'score' task. "
                "The score endpoint is not available for this model."
            )

    def _validate_openai_serving_tokenization(self) -> Optional[ErrorResponse]:
        if self._oai_serving_tokenization is None:
            return self._make_error(
                "This model does not support the 'tokenization' task. "
                "The tokenization endpoint is not available for this model."
            )

    def _validate_engine_client(self):
        assert hasattr(
            self._engine_client, "check_health"
        ), "engine_client must have a check_health attribute"

    def _prepare_engine_config(
        self, callback_ctx: CallbackCtx
    ) -> Tuple["AsyncEngineArgs", "FrontendArgs", "VllmConfig"]:
        """Prepare the engine config to start the engine.

        Args:
            callback_ctx: The callback context.

        Returns:
            A tuple of:
                engine_args: The vLLM's internal engine arguments that is flattened.
                frontend_args: The vLLM's internal frontend arguments that is flattened.
                engine_config: The vLLM's internal engine config that is nested.
        """

        engine_config: VLLMEngineConfig = self.llm_config.get_engine_config()

        # If the backend is anything other than CPU, we need to create the
        # engine config on a task with hardware access.
        if engine_config.accelerator.requires_remote_initialization:
            accelerator = engine_config.accelerator
            accelerator_type = self.llm_config.accelerator_type

            # Initialize options required for the remote task and hardware backend
            remote_options = {
                "num_cpus": 0,
                "runtime_env": callback_ctx.runtime_env,
                "scheduling_strategy": PlacementGroupSchedulingStrategy(
                    placement_group=callback_ctx.placement_group,
                ),
                **accelerator.get_remote_options(accelerator_type),
            }

            ref = (
                ray.remote(_get_vllm_engine_config)
                .options(**remote_options)
                .remote(self.llm_config)
            )
            vllm_engine_args, vllm_engine_config = ray.get(ref)
        else:
            vllm_engine_args, vllm_engine_config = _get_vllm_engine_config(
                self.llm_config
            )

        vllm_frontend_args = FrontendArgs(**engine_config.frontend_kwargs)
        return vllm_engine_args, vllm_frontend_args, vllm_engine_config

    def _start_async_llm_engine(
        self,
        vllm_engine_args: "AsyncEngineArgs",
        vllm_engine_config: "VllmConfig",
        placement_group: PlacementGroup,
    ) -> "EngineClient":
        """Creates an async LLM engine from the engine arguments."""

        from vllm.v1.engine.async_llm import AsyncLLM
        from vllm.v1.executor.abstract import Executor

        vllm_engine_config.parallel_config.placement_group = placement_group

        _clear_current_platform_cache()

        custom_stat_loggers = None
        if self.llm_config.log_engine_metrics:
            from vllm.v1.metrics.ray_wrappers import RayPrometheusStatLogger

            # V1 AsyncLLM does not yet support add_logger: https://github.com/vllm-project/vllm/issues/17702
            # Use `disable_log_stats: False` and `log_engine_metrics: False` as
            # a workaround to enable PrometheusStatLogger instead.
            custom_stat_loggers = [RayPrometheusStatLogger]

        executor_class = Executor.get_class(vllm_engine_config)
        logger.info(f"Using executor class: {executor_class}")
        # Report per-request token progress to the deployment's KV router actor,
        # but only on KV-aware deployments: elsewhere the actor never exists and
        # resolving it per request would block the engine's event loop.
        engine_cls = AsyncLLM
        if is_kv_aware(self.llm_config):
            select_reserve = self.llm_config.experimental_configs.get(
                KV_SELECT_RESERVE_KEY
            )
            if isinstance(select_reserve, str):
                select_reserve = select_reserve.lower() not in (
                    "",
                    "0",
                    "false",
                    "no",
                )
            else:
                select_reserve = bool(select_reserve)
            engine_cls = enable_token_tracking(
                AsyncLLM, select_reserve=select_reserve
            )
        engine_client = engine_cls(
            vllm_config=vllm_engine_config,
            executor_class=executor_class,
            log_requests=vllm_engine_args.enable_log_requests,
            log_stats=not vllm_engine_args.disable_log_stats,
            stat_loggers=custom_stat_loggers,
        )

        return engine_client

    async def resolve_lora(self, disk_lora_model: DiskMultiplexConfig):
        from vllm.entrypoints.serve.lora.protocol import LoadLoRAAdapterRequest

        self._validate_openai_serving_models()

        if disk_lora_model.model_id in self._oai_models.lora_requests:
            # Lora is already loaded, return
            return

        lora_request = await self._oai_models.load_lora_adapter(  # type: ignore[attr-defined]
            request=LoadLoRAAdapterRequest(
                lora_name=disk_lora_model.model_id,
                lora_path=disk_lora_model.local_path,
            )
        )

        if isinstance(lora_request, VLLMErrorResponse):
            raise ValueError(f"Failed to load lora model: {lora_request.error.message}")

    @staticmethod
    def _make_error_response(
        serving: Any,
        exc: Exception,
    ) -> ErrorResponse:
        """Convert an exception to an ErrorResponse and map exception types to
        the appropriate HTTP status codes (e.g. VLLMValidationError -> 400).
        """
        try:
            vllm_error = serving.create_error_response(exc)
            return ErrorResponse(error=ErrorInfo(**vllm_error.error.model_dump()))
        except Exception:
            raise exc  # re-raise the original so it surfaces as a 500

    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        if error := self._validate_openai_serving_chat():
            yield error
            return

        raw_request_info = _canonicalize_request_id_header(request, raw_request_info)
        raw_request: Optional[Request] = RawRequestInfo.to_starlette_request_optional(
            raw_request_info
        )
        try:
            chat_response = await self._oai_serving_chat.create_chat_completion(  # type: ignore[attr-defined]
                request,
                raw_request=raw_request,
            )
        except ValueError as e:
            yield self._make_error_response(self._oai_serving_chat, e)
            return

        if isinstance(chat_response, AsyncGenerator):
            async for response in chat_response:
                if not isinstance(response, str):
                    raise ValueError(
                        f"Expected create_chat_completion to return a stream of strings, got an item with type {type(response)}"
                    )
                yield response
        else:
            if isinstance(chat_response, VLLMErrorResponse):
                yield ErrorResponse(error=ErrorInfo(**chat_response.error.model_dump()))
            else:
                yield ChatCompletionResponse(**chat_response.model_dump())

    async def completions(
        self,
        request: CompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        if error := self._validate_openai_serving_completion():
            yield error
            return

        raw_request_info = _canonicalize_request_id_header(request, raw_request_info)
        raw_request: Optional[Request] = RawRequestInfo.to_starlette_request_optional(
            raw_request_info
        )
        try:
            completion_response = await self._oai_serving_completion.create_completion(  # type: ignore[attr-defined]
                request,
                raw_request=raw_request,
            )
        except ValueError as e:
            yield self._make_error_response(self._oai_serving_completion, e)
            return

        if isinstance(completion_response, AsyncGenerator):
            async for response in completion_response:
                if not isinstance(response, str):
                    raise ValueError(
                        f"Expected create_completion to return a stream of strings, got an item with type {type(response)}"
                    )
                yield response
        else:
            if isinstance(completion_response, VLLMErrorResponse):
                yield ErrorResponse(
                    error=ErrorInfo(**completion_response.error.model_dump())
                )
            else:
                yield CompletionResponse(**completion_response.model_dump())

    async def embeddings(
        self,
        request: EmbeddingRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None]:
        if error := self._validate_openai_serving_embedding():
            yield error
            return

        raw_request_info = _canonicalize_request_id_header(request, raw_request_info)
        raw_request: Optional[Request] = RawRequestInfo.to_starlette_request_optional(
            raw_request_info
        )
        try:
            embedding_response = await self._oai_serving_embedding(
                request,
                raw_request=raw_request,
            )
        except ValueError as e:
            yield self._make_error_response(self._oai_serving_embedding, e)
            return

        # vLLM 0.18+ returns a starlette Response object
        content = json.loads(embedding_response.body)
        yield EmbeddingResponse(**content)

    async def transcriptions(
        self,
        request: TranscriptionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, TranscriptionResponse, ErrorResponse], None]:
        if error := self._validate_openai_serving_transcription():
            yield error
            return

        # Extract audio data from the request file
        audio_data = await request.file.read()

        raw_request_info = _canonicalize_request_id_header(request, raw_request_info)
        raw_request: Optional[Request] = RawRequestInfo.to_starlette_request_optional(
            raw_request_info
        )
        try:
            transcription_response = await self._oai_serving_transcription.create_transcription(  # type: ignore[attr-defined]
                audio_data,
                request,
                raw_request=raw_request,
            )
        except ValueError as e:
            yield self._make_error_response(self._oai_serving_transcription, e)
            return

        if isinstance(transcription_response, AsyncGenerator):
            async for response in transcription_response:
                if not isinstance(response, str):
                    raise ValueError(
                        f"Expected create_transcription to return a stream of strings, got an item with type {type(response)}"
                    )
                yield response
        else:
            if isinstance(transcription_response, VLLMErrorResponse):
                yield ErrorResponse(
                    error=ErrorInfo(**transcription_response.error.model_dump())
                )
            else:
                yield TranscriptionResponse(**transcription_response.model_dump())

    async def score(
        self,
        request: ScoreRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[ScoreResponse, ErrorResponse], None]:
        if error := self._validate_openai_serving_scores():
            yield error
            return

        raw_request_info = _canonicalize_request_id_header(request, raw_request_info)
        raw_request: Optional[Request] = RawRequestInfo.to_starlette_request_optional(
            raw_request_info
        )
        try:
            assert self._oai_serving_scores is not None
            score_response = await self._oai_serving_scores(
                request,
                raw_request=raw_request,
            )
        except ValueError as e:
            yield self._make_error_response(self._oai_serving_scores, e)
            return

        content = json.loads(score_response.body)
        yield ScoreResponse(**content)

    async def tokenize(
        self,
        request: TokenizeRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[TokenizeResponse, ErrorResponse], None]:
        if error := self._validate_openai_serving_tokenization():
            yield error
            return

        raw_request_info = _canonicalize_request_id_header(request, raw_request_info)
        raw_request: Optional[Request] = RawRequestInfo.to_starlette_request_optional(
            raw_request_info
        )
        try:
            tokenize_response = await self._oai_serving_tokenization.create_tokenize(
                request,
                raw_request=raw_request,
            )
        except ValueError as e:
            yield self._make_error_response(self._oai_serving_tokenization, e)
            return

        if isinstance(tokenize_response, VLLMErrorResponse):
            yield ErrorResponse(error=ErrorInfo(**tokenize_response.error.model_dump()))
        else:
            yield TokenizeResponse(**tokenize_response.model_dump())

    async def detokenize(
        self,
        request: DetokenizeRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[DetokenizeResponse, ErrorResponse], None]:
        if error := self._validate_openai_serving_tokenization():
            yield error
            return

        raw_request_info = _canonicalize_request_id_header(request, raw_request_info)
        raw_request: Optional[Request] = RawRequestInfo.to_starlette_request_optional(
            raw_request_info
        )
        try:
            detokenize_response = (
                await self._oai_serving_tokenization.create_detokenize(
                    request,
                    raw_request=raw_request,
                )
            )
        except ValueError as e:
            yield self._make_error_response(self._oai_serving_tokenization, e)
            return

        if isinstance(detokenize_response, VLLMErrorResponse):
            yield ErrorResponse(
                error=ErrorInfo(**detokenize_response.error.model_dump())
            )
        else:
            yield DetokenizeResponse(**detokenize_response.model_dump())

    async def check_health(self) -> None:
        assert self._engine_client is not None, "engine_client is not initialized"

        try:
            await self._engine_client.check_health()
        except BaseException as e:
            logger.error("Healthcheck failed. The replica will be restarted")
            raise e from None

    async def reset_prefix_cache(self) -> None:
        assert self._engine_client is not None, "engine_client is not initialized"
        await self._engine_client.reset_prefix_cache()

    async def sleep(self, **kwargs: Any) -> None:
        """Put the vLLM engine to sleep.

        Args:
            **kwargs: Options parsed into VLLMSleepConfig.
                - level (int): Sleep level (1 or 2). Default 1.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        config = VLLMSleepConfig(**kwargs)
        await self._engine_client.sleep(level=config.level)

    async def wakeup(self, **kwargs: Any) -> None:
        """Wake up the vLLM engine from sleep mode.

        Args:
            **kwargs: Options parsed into VLLMWakeupConfig.
                - tags (List[str], optional): Components to wake up.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        config = VLLMWakeupConfig(**kwargs)
        await self._engine_client.wake_up(tags=config.tags)

    async def is_sleeping(self) -> bool:
        """Check whether the vLLM engine is currently sleeping.

        Returns:
            True if the engine is sleeping, False otherwise.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        return await self._engine_client.is_sleeping()

    async def pause(self, **kwargs: Any) -> None:
        """Pause generation on the vLLM engine.

        This halts generation/encoding requests while keeping model weights
        in GPU memory. New requests are blocked until resume is called.

        Args:
            **kwargs: Options parsed into VLLMPauseConfig.
                - mode (str): "abort" (default), "wait", or "keep".
                - clear_cache (bool): Clear KV cache after draining. Default True.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        config = VLLMPauseConfig(**kwargs)
        await self._engine_client.pause_generation(
            mode=config.mode,
            clear_cache=config.clear_cache,
        )

    async def resume(self, **kwargs: Any) -> None:
        """Resume generation on the vLLM engine after pause.

        Args:
            **kwargs: Reserved for future options.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        await self._engine_client.resume_generation()

    async def is_paused(self) -> bool:
        """Check whether the vLLM engine is currently paused.

        Returns:
            True if the engine is paused, False otherwise.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        return await self._engine_client.is_paused()

    async def start_profile(self) -> None:
        assert self._engine_client is not None, "engine_client is not initialized"
        await self._engine_client.start_profile()

    async def stop_profile(self) -> None:
        assert self._engine_client is not None, "engine_client is not initialized"
        await self._engine_client.stop_profile()

    async def collective_rpc(
        self,
        method: str,
        timeout: Optional[float] = None,
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> list:
        """Execute a collective RPC call on all vLLM workers.

        This is used for RLHF workflows where a trainer needs to execute
        methods on all TP/PP workers (e.g., for weight synchronization).

        Args:
            method: Name of the worker method to execute.
            timeout: Maximum time in seconds to wait for execution.
            args: Positional arguments to pass to the worker method.
            kwargs: Keyword arguments to pass to the worker method.

        Returns:
            A list containing the results from each worker.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        return await self._engine_client.collective_rpc(
            method=method,
            timeout=timeout,
            args=args,
            kwargs=kwargs or {},
        )
