"""Prepare Multimodal Stage"""

import asyncio
from typing import Any, AsyncIterator, Dict, List

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF


class PrepareMultimodalUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        model: str,
        chat_template_content_format: str,
    ):
        """
        Initialize the PrepareMultimodalUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for the multimodal processor.
            chat_template_content_format: The format to render message content.
        """
        super().__init__(data_column, expected_input_keys)

        try:
            from vllm.config import ModelConfig
        except ImportError as e:
            raise ImportError(
                "vLLM is not installed or failed to import. Please run "
                "`pip install ray[llm]` to install required dependencies."
            ) from e

        self.model_config = ModelConfig(model=model)
        self.chat_template_content_format = chat_template_content_format

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Process multimodal data from input messages.

        Args:
            batch: A list of rows to process.

        Yields:
            Dict[str, Any]: A dictionary containing the multimodal data
            along with processing metadata.
        """
        try:
            from vllm.entrypoints.chat_utils import parse_chat_messages_futures
        except ImportError as e:
            raise ImportError(
                "vLLM is not installed or failed to import. Please run "
                "`pip install ray[llm]` to install required dependencies."
            ) from e

        async def _get_mm_data(row: Dict[str, Any], conversation, fut, uuid):
            multimodal_data = await fut
            return row, conversation, uuid, multimodal_data

        tasks = []
        for row in batch:
            # Users can provide stable IDs for each multimodal item from messages to
            # enable engine to cache and reuse work across requests.
            conversation, mm_data_future, mm_uuids = parse_chat_messages_futures(
                row["messages"],
                self.model_config,
                None,  # Tokenizer is not used in vLLM's parse_chat_messages_futures
                content_format=self.chat_template_content_format,
            )
            tasks.append(
                asyncio.create_task(
                    _get_mm_data(row, conversation, mm_data_future, mm_uuids)
                )
            )

        for task in asyncio.as_completed(tasks):
            row, conversation, uuid, multimodal_data = await task

            output = {
                k: v
                for k, v in row.items()
                if k not in ("messages", self.IDX_IN_BATCH_COLUMN)
            }
            output.update(
                {
                    self.IDX_IN_BATCH_COLUMN: row[self.IDX_IN_BATCH_COLUMN],
                    "multimodal_data": multimodal_data,
                    # Use the parsed conversation which has placeholders embedded instead of the original messages
                    "messages": conversation,
                    "multimodal_uuids": uuid,
                }
            )
            yield output


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
