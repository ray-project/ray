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
        input_column: str,
        output_column: str,
        carry_over: bool,
        model: str,
    ):
        """
        Initialize the HttpRequestUDF.

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
        Apply chat template to the given batch.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows with the chat template applied.
        """
        for prompt in self.tokenizer.apply_chat_template(
            [row["messages"].tolist() for row in batch],
            tokenize=False,
            add_generation_prompt=True,
        ):
            yield {"prompt": prompt}

    @property
    def expected_input_keys(self) -> Dict[str, StatefulStageUDF.InputKeyType]:
        """The expected input keys."""
        return {"messages": StatefulStageUDF.InputKeyType.REQUIRED}


class ChatTemplateStage(StatefulStage):
    """
    A stage that applies chat template.
    """

    fn: StatefulStageUDF = ChatTemplateUDF
    fn_constructor_kwargs: Dict[str, Any]
    map_batches_kwargs: Dict[str, Any] = dict(
        concurrency=1,
    )
