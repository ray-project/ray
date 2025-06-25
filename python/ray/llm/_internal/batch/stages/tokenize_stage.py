"""Tokenize and detokenize stage"""

from typing import Any, AsyncIterator, Dict, List, Type

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.utils import get_cached_tokenizer
from ray.llm._internal.common.utils.download_utils import (
    NodeModelDownloadable,
    download_model_files,
)


class TokenizeUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        model: str,
    ):
        """
        Initialize the TokenizeUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for the chat template.
        """
        from transformers import AutoTokenizer

        super().__init__(data_column, expected_input_keys)
        model_path = download_model_files(
            model_id=model,
            mirror_config=None,
            download_model=NodeModelDownloadable.TOKENIZER_ONLY,
            download_extra_files=False,
        )
        self.tokenizer = get_cached_tokenizer(
            AutoTokenizer.from_pretrained(
                model_path,
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


class TokenizeStage(StatefulStage):
    """
    A stage that tokenizes the input.
    """

    fn: Type[StatefulStageUDF] = TokenizeUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        return {"prompt": "The text prompt (str) to tokenize."}


class DetokenizeUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        model: str,
    ):
        """
        Initialize the DetokenizeUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for the chat template.
        """
        from transformers import AutoTokenizer

        super().__init__(data_column, expected_input_keys)
        model_path = download_model_files(
            model_id=model,
            mirror_config=None,
            download_model=NodeModelDownloadable.TOKENIZER_ONLY,
            download_extra_files=False,
        )
        self.tokenizer = get_cached_tokenizer(
            AutoTokenizer.from_pretrained(
                model_path,
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


class DetokenizeStage(StatefulStage):
    """
    A stage that detokenizes the input.
    """

    fn: Type[StatefulStageUDF] = DetokenizeUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        return {"generated_tokens": "A list of generated tokens (int) to detokenize."}
