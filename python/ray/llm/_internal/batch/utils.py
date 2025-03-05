"""Utility functions for batch processing."""
import logging
import os
from typing import TYPE_CHECKING, Any, Optional, Union

from ray.llm._internal.batch.s3_utils import S3Model

if TYPE_CHECKING:
    from transformers import PreTrainedTokenizer, PreTrainedTokenizerFast

AnyTokenizer = Union["PreTrainedTokenizer", "PreTrainedTokenizerFast", Any]

logger = logging.getLogger(__name__)


def get_cached_tokenizer(tokenizer: AnyTokenizer) -> AnyTokenizer:
    """Get tokenizer with cached properties.
    This will patch the tokenizer object in place.
    By default, transformers will recompute multiple tokenizer properties
    each time they are called, leading to a significant slowdown. This
    function caches these properties for faster access.
    Args:
        tokenizer: The tokenizer object.
    Returns:
        The patched tokenizer object.
    """
    chat_template = getattr(tokenizer, "chat_template", None)
    # For VLM, the text tokenizer is wrapped by a processor.
    if hasattr(tokenizer, "tokenizer"):
        tokenizer = tokenizer.tokenizer
        # Some VLM's tokenizer has chat_template attribute (e.g. Qwen/Qwen2-VL-7B-Instruct),
        # however some other VLM's tokenizer does not have chat_template attribute (e.g.
        # mistral-community/pixtral-12b). Therefore, we cache the processor's chat_template.
        if chat_template is None:
            chat_template = getattr(tokenizer, "chat_template", None)

    tokenizer_all_special_ids = set(tokenizer.all_special_ids)
    tokenizer_all_special_tokens_extended = tokenizer.all_special_tokens_extended
    tokenizer_all_special_tokens = set(tokenizer.all_special_tokens)
    tokenizer_len = len(tokenizer)

    class CachedTokenizer(tokenizer.__class__):  # type: ignore
        @property
        def all_special_ids(self):
            return tokenizer_all_special_ids

        @property
        def all_special_tokens(self):
            return tokenizer_all_special_tokens

        @property
        def all_special_tokens_extended(self):
            return tokenizer_all_special_tokens_extended

        @property
        def chat_template(self):
            return chat_template

        def __len__(self):
            return tokenizer_len

    CachedTokenizer.__name__ = f"Cached{tokenizer.__class__.__name__}"

    tokenizer.__class__ = CachedTokenizer
    return tokenizer


def maybe_pull_model_tokenizer_from_s3(model_source: str) -> str:
    """If the model source is a s3 path, pull the model to the local
    directory and return the local path.

    Args:
        model_source: The model source path.

    Returns:
        The local path to the model if it is a s3 path, otherwise the model source path.
    """
    if not model_source.startswith("s3://"):
        return model_source

    s3_tokenizer = S3Model()
    s3_tokenizer.pull_files(
        model_source, ignore_pattern=["*.pt", "*.safetensors", "*.bin"]
    )
    return s3_tokenizer.dir


def maybe_pull_lora_adapter_from_s3(
    lora_name: str,
    s3_path: Optional[str] = None,
) -> str:
    """If the lora name is a s3 path, pull the lora to the local
    directory and return the local path.

    Args:
        lora_name: The lora name.
        s3_path: The S3 path to the lora. If specified, the s3_path will be
            used as the base path to load the lora.

    Returns:
        The local path to the lora if it is a s3 path, otherwise the lora name.
    """
    assert not lora_name.startswith("s3://"), "lora_name cannot be a s3 path"

    if s3_path is None:
        return lora_name

    # Note that we set remove_on_close to False to keep the LoRA adapter
    # in the local directory even after the S3Model object is deleted,
    # because the LoRA adapter will be loaded by vLLM when the request is
    # actually being processed.
    lora_name = os.path.join(s3_path, lora_name)
    s3_lora = S3Model(remove_on_close=False)
    s3_lora.pull_files(lora_name)
    return s3_lora.dir
