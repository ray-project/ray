"""Tokenize stage for the SGLang score task."""

from typing import Any, AsyncIterator, Dict, List, Optional, Type

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.stages.common import is_missing
from ray.llm._internal.batch.stages.tokenize_stage import TokenizeUDF


class SGLangScoreTokenizeUDF(TokenizeUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        model: str,
        trust_remote_code: bool = False,
        add_special_tokens: bool = True,
        enable_mis: bool = False,
        item_first: bool = False,
        score_extraction_token_id: Optional[int] = None,
    ):
        """Tokenize the `query` and `items` text columns of score rows.

        The token IDs match what SGLang's `Engine.score` builds from the same
        text, so scores don't depend on where tokenization runs:

        - With multi-item scoring, or with embedding overrides in the row, the
          query is tokenized with special tokens and each item without.
        - Otherwise, each `query + item` text is tokenized as one sequence and
          `query_token_ids` is empty, because the engine concatenates the two.
          With score extraction, a query containing the extraction token is
          rejected, as SGLang does, since its anchors would be scored as items.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for tokenization.
            trust_remote_code: Whether to trust remote code when loading the model.
            add_special_tokens: Whether to add the tokenizer's special tokens.
            enable_mis: Whether the engine runs multi-item scoring.
            item_first: Whether each item goes before the query.
            score_extraction_token_id: The score extraction token ID, if set.
        """
        super().__init__(data_column, expected_input_keys, model, trust_remote_code)
        self.add_special_tokens = add_special_tokens
        self.enable_mis = enable_mis
        self.item_first = item_first
        self.score_extraction_token_id = score_extraction_token_id

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """Tokenize a batch of score rows.

        Args:
            batch: A list of rows to tokenize.

        Yields:
            Dict[str, Any]: Rows with `query_token_ids` and `item_token_ids`.
        """
        # Texts tokenized with and without special tokens, each in one call.
        special_texts: List[str] = []
        plain_texts: List[str] = []
        # Per row: whether the query is tokenized on its own, and the items.
        plans = []
        # Concatenated queries that must not contain the extraction token.
        anchor_checked_queries: List[str] = []
        for row in batch:
            query = _as_text(row["query"], "query")
            items = [_as_text(item, "items") for item in _as_text_list(row["items"])]
            separate = self.enable_mis or _has_embed_overrides(row)
            if separate:
                special_texts.append(query)
                plain_texts.extend(items)
            else:
                if self.score_extraction_token_id is not None and query:
                    anchor_checked_queries.append(query)
                special_texts.extend(
                    f"{item}{query}" if self.item_first else f"{query}{item}"
                    for item in items
                )
            plans.append((separate, len(items)))

        for query_ids in self._tokenize(
            anchor_checked_queries, self.add_special_tokens
        ):
            if self.score_extraction_token_id in query_ids:
                raise ValueError(
                    "The query contains the score extraction token "
                    f"{self.score_extraction_token_id}; it must only appear in "
                    "items."
                )

        special_ids = self._tokenize(special_texts, self.add_special_tokens)
        plain_ids = self._tokenize(plain_texts, False)

        special_pos = plain_pos = 0
        for row, (separate, num_items) in zip(batch, plans):
            if separate:
                query_token_ids = special_ids[special_pos]
                special_pos += 1
                item_token_ids = plain_ids[plain_pos : plain_pos + num_items]
                plain_pos += num_items
            else:
                query_token_ids = []
                item_token_ids = special_ids[special_pos : special_pos + num_items]
                special_pos += num_items
            yield {
                self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                "query_token_ids": query_token_ids,
                "item_token_ids": item_token_ids,
            }

    def _tokenize(self, texts: List[str], add_special_tokens: bool) -> List[List[int]]:
        if not texts:
            return []
        token_ids = self.tokenizer(texts, add_special_tokens=add_special_tokens)
        return [list(ids) for ids in token_ids["input_ids"]]


def _has_embed_overrides(row: Dict[str, Any]) -> bool:
    """Whether the row has any query or item embedding overrides."""
    if _is_set(row.get("query_embed_overrides")):
        return True
    item_overrides = row.get("item_embed_overrides")
    if is_missing(item_overrides):
        return False
    return any(_is_set(entry) for entry in item_overrides)


def _is_set(value: Any) -> bool:
    return not is_missing(value) and len(value) > 0


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
