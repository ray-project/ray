"""Apply chat template stage"""

from typing import Any, Dict, AsyncIterator, List, Optional, Type

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)


class ChatTemplateUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        model: str,
        chat_template: Optional[str] = None,
    ):
        """
        Initialize the ChatTemplateUDF.

        Args:
            data_column: The data column name.
            model: The model to use for the chat template.
            chat_template: The chat template in Jinja template format. This is
            usually not needed if the model checkpoint already contains the
            chat template.
        """
        from transformers import AutoProcessor

        super().__init__(data_column)

        # NOTE: We always use processor instead of tokenizer in this stage,
        # because tokenizers of VLM models may not have chat template attribute.
        # However, this may not be a reliable solution, because processors and
        # tokenizers are not standardized across different models.
        self.processor = AutoProcessor.from_pretrained(model, trust_remote_code=True)
        self.chat_template = chat_template

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Apply chat template to the given batch.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows with the chat template applied.
        """
        prompts = []
        for row in batch:
            # PyArrow cannot handle the messages with images, so Ray Data
            # will fallback to use pickle for serialization. In this case,
            # the "messages" column is already a list of dicts and does not
            # have .tolist() method.
            if hasattr(row["messages"], "tolist"):
                conversation = row["messages"].tolist()
            else:
                conversation = row["messages"]
            add_generation_prompt = self._should_add_generation_prompt(conversation)
            # If we don't add a generation prompt, we should continue the final message.
            continue_final_message = not add_generation_prompt
            prompts.append(
                self.processor.apply_chat_template(
                    conversation,
                    tokenize=False,
                    chat_template=self.chat_template,
                    add_generation_prompt=add_generation_prompt,
                    continue_final_message=continue_final_message,
                )
            )
        assert len(batch) == len(prompts)

        for row, prompt in zip(batch, prompts):
            yield {
                self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                "prompt": prompt,
            }

    def _should_add_generation_prompt(self, conversation: List[Dict[str, Any]]) -> bool:
        """Determines if the generation prompt should be added for the given conversation.

        Adds the generation prompt only if the last message is from the user.
        This is useful in cases where the user provides an assistant prefill
        message.

        Args:
            conversation: The conversation to check.

        Returns:
            True if the generation prompt should be added, False otherwise.
        """
        return conversation[-1]["role"] == "user"

    @property
    def expected_input_keys(self) -> List[str]:
        """The expected input keys."""
        return ["messages"]


class ChatTemplateStage(StatefulStage):
    """
    A stage that applies chat template.
    """

    fn: Type[StatefulStageUDF] = ChatTemplateUDF
