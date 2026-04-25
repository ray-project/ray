"""This module contains wrapper classes for OpenAI-compatible protocol models.

Each feature group (chat-completion, completion, embed, score, transcription,
tokenize, error) is resolved independently:
    1. try the vLLM submodule that provides it,
    2. fall back to the SGLang equivalent if vLLM doesn't have that submodule,
    3. fall back to a NotImplementedError stub so import never blows up.

This lets Ray Serve LLM run on top of vLLM builds that ship a partial protocol
surface (e.g., the DeepSeek-V4 cu130 dev container does not have
`vllm.entrypoints.pooling.score`, which previously prevented the entire module
from importing). Only the missing endpoints raise; chat/completion still work.

Chat-completion + completion are required (since every supported engine ships
them); if both vLLM and SGLang are unavailable we still raise ImportError.
"""

import logging
import uuid
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


def _unsupported_model(name: str, hint: str = ""):
    """Create a BaseModel stub that raises NotImplementedError on instantiation."""
    msg = f"{name} is not supported with the current backend." + (
        f" {hint}" if hint else ""
    )

    class _Stub(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)

        def __init__(self, **kwargs):
            raise NotImplementedError(msg)

    _Stub.__name__ = _Stub.__qualname__ = name
    return _Stub


def _try_import(module_path: str, names: tuple) -> Optional[dict]:
    """Try `from module_path import *names`. Return dict of name->obj or None."""
    try:
        mod = __import__(module_path, fromlist=list(names))
    except ImportError:
        return None
    out = {}
    for name in names:
        try:
            out[name] = getattr(mod, name)
        except AttributeError:
            return None
    return out


# --- Chat completion (REQUIRED) -----------------------------------------------
_chat = _try_import(
    "vllm.entrypoints.openai.chat_completion.protocol",
    ("ChatCompletionRequest", "ChatCompletionResponse", "ChatCompletionStreamResponse"),
)
if _chat is None:
    _sg = _try_import(
        "sglang.srt.entrypoints.openai.protocol",
        ("ChatCompletionRequest", "ChatCompletionResponse", "ChatCompletionStreamResponse"),
    )
    if _sg is None:
        raise ImportError(
            "Neither vLLM nor SGLang exposes ChatCompletion protocol models. "
            "At least one is required for Ray Serve LLM. "
            "Install with: `pip install ray[llm]` or `pip install sglang[all]`"
        )
    _chat = _sg
_ChatCompletionRequest = _chat["ChatCompletionRequest"]
_ChatCompletionResponse = _chat["ChatCompletionResponse"]
_ChatCompletionStreamResponse = _chat["ChatCompletionStreamResponse"]

# --- Completion (REQUIRED) ----------------------------------------------------
_cmpl = _try_import(
    "vllm.entrypoints.openai.completion.protocol",
    ("CompletionRequest", "CompletionResponse", "CompletionStreamResponse"),
) or _try_import(
    "sglang.srt.entrypoints.openai.protocol",
    ("CompletionRequest", "CompletionResponse", "CompletionStreamResponse"),
)
if _cmpl is None:
    raise ImportError(
        "Neither vLLM nor SGLang exposes Completion protocol models."
    )
_CompletionRequest = _cmpl["CompletionRequest"]
_CompletionResponse = _cmpl["CompletionResponse"]
_CompletionStreamResponse = _cmpl["CompletionStreamResponse"]

# --- Error (always defined; vLLM uses nested error→info, SGLang has no nest) --
_err = _try_import(
    "vllm.entrypoints.openai.engine.protocol",
    ("ErrorInfo", "ErrorResponse"),
)
if _err is not None:
    _ErrorInfo = _err["ErrorInfo"]
    _ErrorResponse = _err["ErrorResponse"]
else:
    class _ErrorInfo(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        message: str
        type: str
        param: Optional[str] = None
        code: int

    class _ErrorResponse(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        error: _ErrorInfo

# --- Embedding (optional) -----------------------------------------------------
_emb = _try_import(
    "vllm.entrypoints.pooling.embed.protocol",
    ("EmbeddingChatRequest", "EmbeddingCompletionRequest", "EmbeddingResponse"),
)
if _emb is not None:
    _EmbeddingChatRequest = _emb["EmbeddingChatRequest"]
    _EmbeddingCompletionRequest = _emb["EmbeddingCompletionRequest"]
    _EmbeddingResponse = _emb["EmbeddingResponse"]
else:
    _sg = _try_import(
        "sglang.srt.entrypoints.openai.protocol",
        ("EmbeddingRequest", "EmbeddingResponse"),
    )
    if _sg is not None:
        _EmbeddingCompletionRequest = _sg["EmbeddingRequest"]
        _EmbeddingChatRequest = _sg["EmbeddingRequest"]  # SGLang has one shared type
        _EmbeddingResponse = _sg["EmbeddingResponse"]
    else:
        _emb_hint = "Install a vLLM/SGLang build with embedding support."
        _EmbeddingChatRequest = _unsupported_model("EmbeddingChatRequest", _emb_hint)
        _EmbeddingCompletionRequest = _unsupported_model(
            "EmbeddingCompletionRequest", _emb_hint
        )
        _EmbeddingResponse = _unsupported_model("EmbeddingResponse", _emb_hint)
        logger.info(
            "Ray Serve LLM: embedding endpoints unavailable (no vLLM/SGLang "
            "embedding protocol). Calls will raise NotImplementedError."
        )

# --- Score (optional) ---------------------------------------------------------
_score = _try_import(
    "vllm.entrypoints.pooling.score.protocol",
    ("ScoreResponse", "ScoreTextRequest"),
)
if _score is not None:
    _ScoreResponse = _score["ScoreResponse"]
    _ScoreTextRequest = _score["ScoreTextRequest"]
else:
    _sg = _try_import(
        "sglang.srt.entrypoints.openai.protocol",
        ("ScoringRequest", "ScoringResponse"),
    )
    if _sg is not None:
        _ScoreTextRequest = _sg["ScoringRequest"]
        _ScoreResponse = _sg["ScoringResponse"]
    else:
        _score_hint = "Install a vLLM/SGLang build with scoring support."
        _ScoreResponse = _unsupported_model("ScoreResponse", _score_hint)
        _ScoreTextRequest = _unsupported_model("ScoreTextRequest", _score_hint)
        logger.info(
            "Ray Serve LLM: score endpoints unavailable (no vLLM/SGLang "
            "scoring protocol). Calls will raise NotImplementedError."
        )

# --- Transcription (optional, vLLM-only) --------------------------------------
_trn = _try_import(
    "vllm.entrypoints.openai.speech_to_text.protocol",
    (
        "TranscriptionRequest",
        "TranscriptionResponse",
        "TranscriptionStreamResponse",
    ),
)
if _trn is not None:
    _TranscriptionRequest = _trn["TranscriptionRequest"]
    _TranscriptionResponse = _trn["TranscriptionResponse"]
    _TranscriptionStreamResponse = _trn["TranscriptionStreamResponse"]
else:
    _trn_hint = "Install vLLM with speech_to_text support to use transcription endpoints."
    _TranscriptionRequest = _unsupported_model("TranscriptionRequest", _trn_hint)
    _TranscriptionResponse = _unsupported_model("TranscriptionResponse", _trn_hint)
    _TranscriptionStreamResponse = _unsupported_model(
        "TranscriptionStreamResponse", _trn_hint
    )

# --- Tokenize / detokenize (optional) -----------------------------------------
_tok = _try_import(
    "vllm.entrypoints.serve.tokenize.protocol",
    (
        "DetokenizeRequest",
        "DetokenizeResponse",
        "TokenizeChatRequest",
        "TokenizeCompletionRequest",
        "TokenizeResponse",
    ),
)
if _tok is not None:
    _DetokenizeRequest = _tok["DetokenizeRequest"]
    _DetokenizeResponse = _tok["DetokenizeResponse"]
    _TokenizeChatRequest = _tok["TokenizeChatRequest"]
    _TokenizeCompletionRequest = _tok["TokenizeCompletionRequest"]
    _TokenizeResponse = _tok["TokenizeResponse"]
else:
    _sg = _try_import(
        "sglang.srt.entrypoints.openai.protocol",
        (
            "DetokenizeRequest",
            "DetokenizeResponse",
            "TokenizeRequest",
            "TokenizeResponse",
        ),
    )
    if _sg is not None:
        _DetokenizeRequest = _sg["DetokenizeRequest"]
        _DetokenizeResponse = _sg["DetokenizeResponse"]
        _TokenizeCompletionRequest = _sg["TokenizeRequest"]
        _TokenizeChatRequest = _sg["TokenizeRequest"]  # SGLang has one shared type
        _TokenizeResponse = _sg["TokenizeResponse"]
    else:
        _tok_hint = "Install a vLLM/SGLang build with tokenize support."
        _DetokenizeRequest = _unsupported_model("DetokenizeRequest", _tok_hint)
        _DetokenizeResponse = _unsupported_model("DetokenizeResponse", _tok_hint)
        _TokenizeChatRequest = _unsupported_model("TokenizeChatRequest", _tok_hint)
        _TokenizeCompletionRequest = _unsupported_model(
            "TokenizeCompletionRequest", _tok_hint
        )
        _TokenizeResponse = _unsupported_model("TokenizeResponse", _tok_hint)
        logger.info(
            "Ray Serve LLM: tokenize/detokenize endpoints unavailable. "
            "Calls will raise NotImplementedError."
        )


if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


class ChatCompletionRequest(_ChatCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ChatCompletionResponse(_ChatCompletionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ChatCompletionStreamResponse(_ChatCompletionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ErrorInfo(_ErrorInfo):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ErrorResponse(_ErrorResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


# TODO (Kourosh): Upstream
class CompletionRequest(_CompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CompletionResponse(_CompletionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CompletionStreamResponse(_CompletionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


# TODO (Kourosh): Upstream
class EmbeddingCompletionRequest(_EmbeddingCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class EmbeddingChatRequest(_EmbeddingChatRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class EmbeddingResponse(_EmbeddingResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TranscriptionRequest(_TranscriptionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    request_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description=(
            "The request_id related to this request. If the caller does "
            "not set it, a random_uuid will be generated. This id is used "
            "through out the inference process and return in response."
        ),
    )


class TranscriptionResponse(_TranscriptionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TranscriptionStreamResponse(_TranscriptionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ScoreRequest(_ScoreTextRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ScoreResponse(_ScoreResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TokenizeCompletionRequest(_TokenizeCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TokenizeChatRequest(_TokenizeChatRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TokenizeResponse(_TokenizeResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class DetokenizeRequest(_DetokenizeRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class DetokenizeResponse(_DetokenizeResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


EmbeddingRequest = Union[EmbeddingCompletionRequest, EmbeddingChatRequest]

TokenizeRequest = Union[TokenizeCompletionRequest, TokenizeChatRequest]

LLMEmbeddingsResponse = Union[
    AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None],
]

LLMScoreResponse = Union[
    AsyncGenerator[Union[ScoreResponse, ErrorResponse], None],
]

LLMTokenizeResponse = Union[
    AsyncGenerator[Union[TokenizeResponse, ErrorResponse], None],
]

LLMDetokenizeResponse = Union[
    AsyncGenerator[Union[DetokenizeResponse, ErrorResponse], None],
]

LLMChatResponse = Union[
    AsyncGenerator[
        Union[str, ChatCompletionStreamResponse, ChatCompletionResponse, ErrorResponse],
        None,
    ],
]

LLMCompletionsResponse = Union[
    AsyncGenerator[
        Union[str, CompletionStreamResponse, CompletionResponse, ErrorResponse], None
    ],
]

LLMTranscriptionResponse = Union[
    AsyncGenerator[
        Union[str, TranscriptionStreamResponse, TranscriptionResponse, ErrorResponse],
        None,
    ],
]


# TODO: remove this class
class OpenAIHTTPException(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        type: str = "Unknown",
        internal_message: Optional[str] = None,
    ) -> None:
        self.status_code = status_code
        self.message = message
        self.type = type
        self.internal_message = internal_message


# TODO: upstream metadata for ModelData
# Compared to vLLM this has a metadata field.
class ModelCard(BaseModel):
    model_config = ConfigDict(
        protected_namespaces=tuple(), arbitrary_types_allowed=True
    )

    id: str
    object: str
    owned_by: str
    permission: List[str]
    metadata: Dict[str, Any]

    @property
    def model_type(self) -> str:
        return self.metadata["engine_config"]["model_type"]


class ModelList(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    data: List[ModelCard]
    object: str = "list"


def to_model_metadata(
    model_id: str,
    model_config: "LLMConfig",
    overrides: Optional[Dict[str, Any]] = None,
) -> ModelCard:
    """Creates an OpenAI-compatible ModelData object.

    Args:
        model_id: The ID of the model. Should contain the suffix if the model
            is LoRA fine-tuned. For example:
                meta-llama/Llama-2-7b-chat-hf:my_suffix:aBc1234
        model_config: The model's YAML config.
        overrides: should only be set for LoRA fine-tuned models. The
            overrides of the fine-tuned model metadata.

    Returns:
        A ModelCard object.
    """
    metadata = {
        "model_id": model_config.model_id,
        "input_modality": model_config.input_modality,
        "max_request_context_length": model_config.max_request_context_length,
    }

    if overrides:
        metadata.update(overrides)

    return ModelCard(
        id=model_id,
        object="model",
        owned_by="organization-owner",
        permission=[],
        metadata=metadata,
    )
