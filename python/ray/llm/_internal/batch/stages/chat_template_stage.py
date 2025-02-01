"""Apply chat template stage"""

from typing import Any, Dict, AsyncIterator, List

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.utils import get_cached_tokenizer


class ChatTemplateUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        model: str,
    ):
        """
        Initialize the ChatTemplateUDF.

        Args:
            data_column: The data column name.
            model: The model to use for the chat template.
        """
        from transformers import AutoTokenizer

        super().__init__(data_column)
        self.tokenizer = get_cached_tokenizer(AutoTokenizer.from_pretrained(model))

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Apply chat template to the given batch.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows with the chat template applied.
        """
        for row, prompt in zip(
            batch,
            self.tokenizer.apply_chat_template(
                [row["messages"].tolist() for row in batch],
                tokenize=False,
                add_generation_prompt=True,
            ),
        ):
            yield {
                self.idx_in_batch_column: row[self.idx_in_batch_column],
                "prompt": prompt,
            }

    @property
    def expected_input_keys(self) -> List[str]:
        """The expected input keys."""
        return ["messages"]


class ChatTemplateStage(StatefulStage):
    """
    A stage that applies chat template.
    """

    fn: StatefulStageUDF = ChatTemplateUDF
    fn_constructor_kwargs: Dict[str, Any]
    map_batches_kwargs: Dict[str, Any] = dict(
        concurrency=1,
    )
