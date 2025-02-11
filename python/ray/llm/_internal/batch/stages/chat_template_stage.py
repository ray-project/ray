"""Apply chat template stage"""

from typing import Any, Dict, AsyncIterator, List

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)


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
        from transformers import AutoProcessor

        super().__init__(data_column)

        # NOTE: We always use processor instead of tokenizer, because tokenizers
        # of VLM models may not have chat template attribute. However, this may
        # not be a reliable solution, because processors and tokenizers are not
        # standardized across different models.
        self.processor = AutoProcessor.from_pretrained(model)

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Apply chat template to the given batch.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows with the chat template applied.
        """
        prompts = self.processor.apply_chat_template(
            [
                row["messages"].tolist()
                if not isinstance(row["messages"], list)
                else row["messages"]
                for row in batch
            ],
            tokenize=False,
            add_generation_prompt=True,
        )
        assert len(batch) == len(prompts)

        for row, prompt in zip(batch, prompts):
            yield {
                self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
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
