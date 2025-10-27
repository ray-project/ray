"""Prepare Multimodal Stage"""

import asyncio
from typing import Any, AsyncIterator, Dict, List

from vllm.config import ModelConfig
from vllm.entrypoints.chat_utils import parse_chat_messages_futures

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF


class PrepareMultimodalUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        model: str,
    ):
        """
        Initialize the PrepareMultimodalUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for the multimodal processor.
        """
        super().__init__(data_column, expected_input_keys)
        self.model_config = ModelConfig(model=model)

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Process multimodal data from the input messages.

        Args:
            batch: A list of rows to process.

        Returns:
            A generator of rows with the processed multimodal data.
        """
        conversations, futures = [], []
        for row in batch:
            conversation, mm_data_future, _ = parse_chat_messages_futures(
                row["messages"],
                self.model_config,
                None,  # Tokenizer is not used in vLLM's parse_chat_messages_futures
                content_format="string",
            )
            conversations.append(conversation)
            futures.append(mm_data_future)

        async def _get_mm_with_idx(idx: int, fut):
            multimodal_data = await fut
            return idx, multimodal_data

        tasks = [
            asyncio.create_task(_get_mm_with_idx(idx, fut))
            for idx, fut in enumerate(futures)
        ]

        for task in asyncio.as_completed(tasks):
            idx, multimodal_data = await task

            yield {
                self.IDX_IN_BATCH_COLUMN: idx,
                "multimodal_data": multimodal_data or {},
                # Use the parsed conversation which has placeholders embedded instead of the original messages
                "messages": conversations[idx],
            }


class PrepareMultimodalStage(StatefulStage):
    """
    A stage that prepares multimodal data from the input messages for a specific model.
    """

    fn: StatefulStageUDF = PrepareMultimodalUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        return {
            "messages": "A list of messages in OpenAI chat format. "
            "See https://platform.openai.com/docs/api-reference/chat/create "
            "for details."
        }
