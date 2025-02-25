"""Tokenize and detokenize stage"""

from typing import Any, Dict, AsyncIterator, List, Type

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.utils import get_cached_tokenizer


class TokenizeUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        model: str,
    ):
        """
        Initialize the TokenizeUDF.

        Args:
            data_column: The data column name.
            model: The model to use for the chat template.
        """
        from transformers import AutoTokenizer

        super().__init__(data_column)
        self.tokenizer = get_cached_tokenizer(
            AutoTokenizer.from_pretrained(
                model,
                trust_remote_code=True,
            )
        )

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Tokenize the given batch.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows with the tokenized prompt.
        """
        for row, prompt_token_ids in zip(
            batch,
            self.tokenizer([row["prompt"] for row in batch])["input_ids"],
        ):
            yield {
                self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                "tokenized_prompt": prompt_token_ids,
            }

    @property
    def expected_input_keys(self) -> List[str]:
        """The expected input keys."""
        return ["prompt"]


class TokenizeStage(StatefulStage):
    """
    A stage that tokenizes the input.
    """

    fn: Type[StatefulStageUDF] = TokenizeUDF


class DetokenizeUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        model: str,
    ):
        """
        Initialize the DetokenizeUDF.

        Args:
            data_column: The data column name.
            model: The model to use for the chat template.
        """
        from transformers import AutoTokenizer

        super().__init__(data_column)
        self.tokenizer = get_cached_tokenizer(
            AutoTokenizer.from_pretrained(
                model,
                trust_remote_code=True,
            )
        )

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Detokenize the given batch.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows with the detokenized prompt.
        """
        for row, generated_text in zip(
            batch,
            self.tokenizer.batch_decode(
                [row["generated_tokens"] for row in batch],
                skip_special_tokens=True,
            ),
        ):
            yield {
                self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                "generated_text": generated_text,
            }

    @property
    def expected_input_keys(self) -> List[str]:
        """The expected input keys."""
        return ["generated_tokens"]


class DetokenizeStage(StatefulStage):
    """
    A stage that detokenizes the input.
    """

    fn: Type[StatefulStageUDF] = DetokenizeUDF
