"""Tokenize stage for the SGLang score task."""

from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.utils import get_cached_tokenizer
from ray.llm._internal.common.utils.download_utils import (
    NodeModelDownloadable,
    download_model_files,
)

# Same value as `sglang.srt.constants.MIS_DELIMITER_TOKEN_ID`; copied so this
# CPU stage doesn't import SGLang.
_MIS_DELIMITER_TOKEN_ID = 9999


def load_tokenizer(model: str, trust_remote_code: bool = False) -> Any:
    """Download and load the tokenizer for `model`.

    Args:
        model: The model source.
        trust_remote_code: Whether to trust remote code when loading.

    Returns:
        The Hugging Face tokenizer.
    """
    from transformers import AutoTokenizer

    model_path = download_model_files(
        model_id=model,
        mirror_config=None,
        download_model=(
            NodeModelDownloadable.EXCLUDE_SAFETENSORS
            if trust_remote_code
            else NodeModelDownloadable.TOKENIZER_ONLY
        ),
        download_extra_files=False,
    )
    return AutoTokenizer.from_pretrained(
        model_path, trust_remote_code=trust_remote_code
    )


def resolve_embed_override_token(tokenizer: Any, token: str) -> int:
    """Resolve the embedding marker text to a single token ID.

    Args:
        tokenizer: The Hugging Face tokenizer.
        token: The marker text, for example a registered special token.

    Returns:
        The marker's token ID.
    """
    token_ids = list(tokenizer(token, add_special_tokens=False)["input_ids"])
    if len(token_ids) != 1:
        raise ValueError(
            f"'embed_override_token' {token!r} tokenizes to {len(token_ids)} "
            "tokens. Use a single token, such as a registered special token."
        )
    token_id = int(token_ids[0])
    if token_id == getattr(tokenizer, "unk_token_id", None):
        raise ValueError(
            f"'embed_override_token' {token!r} isn't in the tokenizer vocabulary."
        )
    # The marker must stay one token when surrounded by text, so check that
    # it appears exactly once in a few probes. Comparing against the
    # neighbors tokenized alone would fail on SentencePiece tokenizers.
    for probe in (f"a{token}b", f" x {token} y ", f"{token}{token}"):
        expected = probe.count(token)
        probe_ids = tokenizer(probe, add_special_tokens=False)["input_ids"]
        if list(probe_ids).count(token_id) != expected:
            raise ValueError(
                f"'embed_override_token' {token!r} merges with surrounding "
                "text. Register it as a special token."
            )
    return token_id


class SGLangScoreTokenizeUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        model: str,
        trust_remote_code: bool = False,
        add_special_tokens: bool = True,
        embed_override_token: Optional[str] = None,
        embed_override_token_id: Optional[int] = None,
    ):
        """Tokenize the `query` and `items` text columns of score rows.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for tokenization.
            trust_remote_code: Whether to trust remote code when loading the model.
            add_special_tokens: Whether to add the tokenizer's special tokens
                around each query+item sequence.
            embed_override_token: The embedding marker text. If set, each row
                is checked to tokenize every marker to `embed_override_token_id`.
            embed_override_token_id: The token ID the marker resolves to.
        """
        super().__init__(data_column, expected_input_keys)
        self.tokenizer = get_cached_tokenizer(load_tokenizer(model, trust_remote_code))
        # SGLang scores the concatenation query + item, so the special tokens
        # wrap that sequence: the prefix goes on the query, the suffix on items.
        self.query_prefix, self.item_suffix = (
            _sequence_special_tokens(self.tokenizer) if add_special_tokens else ([], [])
        )
        if (embed_override_token is None) != (embed_override_token_id is None):
            raise ValueError(
                "Set both 'embed_override_token' and 'embed_override_token_id', "
                "or neither."
            )
        self.embed_override_token = embed_override_token
        self.embed_override_token_id = embed_override_token_id

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """Tokenize a batch of score rows.

        Args:
            batch: A list of rows to tokenize.

        Yields:
            Dict[str, Any]: Rows with `query_token_ids` and `item_token_ids`.
        """
        queries = [_as_text(row["query"], "query") for row in batch]
        items_per_row = [
            [_as_text(item, "items") for item in _as_text_list(row["items"])]
            for row in batch
        ]

        # Tokenize every query and item in one call, then regroup by row.
        flat_texts = queries + [item for items in items_per_row for item in items]
        flat_token_ids = (
            self.tokenizer(flat_texts, add_special_tokens=False)["input_ids"]
            if flat_texts
            else []
        )
        flat_item_token_ids = flat_token_ids[len(queries) :]

        offset = 0
        for row, query, items, query_ids in zip(
            batch, queries, items_per_row, flat_token_ids
        ):
            item_ids = flat_item_token_ids[offset : offset + len(items)]
            offset += len(items)
            if self.embed_override_token is not None:
                self._check_markers(query, query_ids, "query")
                for text, ids in zip(items, item_ids):
                    self._check_markers(text, ids, "items")
            yield {
                self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                "query_token_ids": self.query_prefix + list(query_ids),
                "item_token_ids": [list(ids) + self.item_suffix for ids in item_ids],
            }

    def _check_markers(self, text: str, token_ids: List[int], column: str) -> None:
        """Raise if a marker in `text` didn't tokenize to the marker token ID."""
        expected = text.count(self.embed_override_token)
        actual = list(token_ids).count(self.embed_override_token_id)
        if actual != expected:
            raise ValueError(
                f"Marker {self.embed_override_token!r} occurs {expected} times in "
                f"'{column}' text but {actual} times in its token IDs. Register "
                "it as a special token."
            )


def _sequence_special_tokens(tokenizer: Any) -> Tuple[List[int], List[int]]:
    """Return the special tokens the tokenizer adds before and after a sequence.

    Found by tokenizing a probe with and without special tokens, because fast
    tokenizers add them in a post-processor rather than a method we can call.
    """
    core = list(tokenizer("hello", add_special_tokens=False)["input_ids"])
    wrapped = list(tokenizer("hello", add_special_tokens=True)["input_ids"])
    for index in range(len(wrapped) - len(core) + 1):
        if core and wrapped[index : index + len(core)] == core:
            return wrapped[:index], wrapped[index + len(core) :]
    raise ValueError(
        f"Can't find the special tokens {type(tokenizer).__name__} adds. "
        "Set add_special_tokens=False."
    )


def _as_text_list(value: Any) -> List[Any]:
    """Return `value` as a list, or raise if it isn't a sequence of items."""
    if hasattr(value, "tolist"):
        value = value.tolist()
    if not isinstance(value, (list, tuple)):
        raise ValueError(
            f"'items' must be a list of strings, got {type(value).__name__}."
        )
    return list(value)


def _as_text(value: Any, column: str) -> str:
    """Return `value` if it is a string, or raise."""
    if not isinstance(value, str):
        raise ValueError(
            f"'{column}' must be text, got {type(value).__name__}. Set "
            "tokenize_stage=False and pass 'query_token_ids' and "
            "'item_token_ids' to score pre-tokenized input."
        )
    return value


class SGLangScoreTokenizeStage(StatefulStage):
    """A stage that tokenizes the query and items of score rows."""

    fn: Type[StatefulStageUDF] = SGLangScoreTokenizeUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        return {
            "query": "The query text (str).",
            "items": "The item texts (List[str]).",
        }
