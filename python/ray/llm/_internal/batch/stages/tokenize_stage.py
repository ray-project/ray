"""Tokenize and detokenize stage"""

from typing import Any, Dict, AsyncIterator, List

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.utils import get_cached_tokenizer


class TokenizeUDF(StatefulStageUDF):
    def __init__(
        self,
        input_column: str,
        output_column: str,
        carry_over: bool,
        model: str,
    ):
        """
        Initialize the TokenizeUDF.

        Args:
            input_column: The input column name.
            output_column: The output column name.
            carry_over: Whether to carry over the input column to the output column.
            model: The model to use for the chat template.
        """
        from transformers import AutoTokenizer

        super().__init__(input_column, output_column, carry_over)
        self.tokenizer = get_cached_tokenizer(AutoTokenizer.from_pretrained(model))

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
            yield {"prompt": row["prompt"], "tokenized_prompt": prompt_token_ids}

    @property
    def expected_input_keys(self) -> Dict[str, StatefulStageUDF.InputKeyType]:
        """The expected input keys."""
        return {"prompt": StatefulStageUDF.InputKeyType.REQUIRED}


class TokenizeStage(StatefulStage):
    """
    A stage that tokenizes the input.
    """

    fn: StatefulStageUDF = TokenizeUDF
    fn_constructor_kwargs: Dict[str, Any]
    map_batches_kwargs: Dict[str, Any] = dict(
        concurrency=1,
    )


class DetokenizeUDF(StatefulStageUDF):
    def __init__(
        self,
        input_column: str,
        output_column: str,
        carry_over: bool,
        model: str,
    ):
        """
        Initialize the DetokenizeUDF.

        Args:
            input_column: The input column name.
            output_column: The output column name.
            carry_over: Whether to carry over the input column to the output column.
            model: The model to use for the chat template.
        """
        from transformers import AutoTokenizer

        super().__init__(input_column, output_column, carry_over)
        self.tokenizer = get_cached_tokenizer(AutoTokenizer.from_pretrained(model))

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
                "generated_tokens": row["generated_tokens"],
                "generated_text": generated_text,
            }

    @property
    def expected_input_keys(self) -> Dict[str, StatefulStageUDF.InputKeyType]:
        """The expected input keys."""
        return {"generated_tokens": StatefulStageUDF.InputKeyType.REQUIRED}


class DetokenizeStage(StatefulStage):
    """
    A stage that detokenizes the input.
    """

    fn: StatefulStageUDF = DetokenizeUDF
    fn_constructor_kwargs: Dict[str, Any]
    map_batches_kwargs: Dict[str, Any] = dict(
        concurrency=1,
    )
