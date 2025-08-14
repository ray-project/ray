"""Utility functions for batch processing."""
import logging
from typing import TYPE_CHECKING, Any, Union

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
