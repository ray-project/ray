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
        apply_sys_msg_formatting: bool = False,
    ):
        """
        Initialize the PrepareMultimodalUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for the multimodal processor.
            chat_template_content_format: The format to render message content.
            apply_sys_msg_formatting: Whether to skip formatting system messages.
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
        self.apply_sys_msg_formatting = apply_sys_msg_formatting

    def _extract_system_messages(
        self, messages: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract system messages from the message list.

        System messages are kept as strings (not converted to list format) to avoid
        issues with chat templates that expect string system messages, e.g. Pixtral.

        Args:
            messages: The full message list.

        Returns:
            A tuple of (system_messages, non_system_messages).
        """
        system_messages = []
        non_system_messages = []

        for msg in messages:
            if msg.get("role") == "system":
                system_content = msg.get("content")
                if isinstance(system_content, list):
                    text_parts = []
                    for part in system_content:
                        if isinstance(part, dict) and part.get("type") == "text":
                            text_value = part.get("text") or part.get("content")
                            if text_value:
                                text_parts.append(str(text_value))
                        elif isinstance(part, str) and part:
                            text_parts.append(part)
                    system_content = "\n".join(text_parts) if text_parts else ""

                system_messages.append({**msg, "content": system_content})
            else:
                non_system_messages.append(msg)

        return system_messages, non_system_messages

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
            # Extract system messages to keep them as strings (not converted to list format)
            # This avoids issues with chat templates that expect string system messages.
            system_messages = []
            messages_to_parse = row["messages"]

            if self.apply_sys_msg_formatting:
                system_messages, messages_to_parse = self._extract_system_messages(
                    row["messages"]
                )

            # Users can provide stable IDs for each multimodal item from messages to
            # enable engine to cache and reuse work across requests.
            conversation, mm_data_future, mm_uuids = parse_chat_messages_futures(
                messages_to_parse,
                self.model_config,
                None,  # Tokenizer is not used in vLLM's parse_chat_messages_futures
                content_format=self.chat_template_content_format,
            )

            if system_messages:
                conversation = system_messages + conversation

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
