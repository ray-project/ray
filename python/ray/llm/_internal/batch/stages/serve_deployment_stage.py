"""The stage that runs serve deployment."""

import logging
import time
import uuid

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray import serve
from typing import Any, AsyncIterator, Dict, List, Type, Union, Tuple
import asyncio
from ray.llm._internal.serve.configs.openai_api_models import (
    CompletionRequest,
    ChatCompletionRequest,
)

logger = logging.getLogger(__name__)


class ServeDeploymentStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        deployment_name: str,
        app_name: str,
        method: str,
    ):
        """
        Initialize the ServeDeploymentStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            deployment_name: The name of the deployment.
            app_name: The name of the deployment app.
            method: The method to call on the deployment.
        """
        super().__init__(data_column, expected_input_keys)
        self._dh = serve.get_deployment_handle(deployment_name, app_name).options(
            stream=True
        )
        self._method = method
        self._request_type = self._resolve_request_type()
        self.request_id = 0

    def _prepare_request(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decorate the request with metadata related to the batch.

        Args:
            row: The row.

        Returns:
            The decorated request.
        """
        request = {
            "request_id": str(self.request_id),
            "idx_in_batch": row[self.IDX_IN_BATCH_COLUMN],
            **row,
        }
        self.request_id += 1
        return request

    def _resolve_request_type(self) -> Union[ChatCompletionRequest, CompletionRequest]:
        """
        Resolve the request type based on the method.

        Returns:
            The request type.
        """
        if getattr(self._dh, self._method) is None:
            raise ValueError(
                f"Method {self._method} is not supported by the serve deployment."
            )

        if self._method == "chat":
            return ChatCompletionRequest
        elif self._method == "completions":
            return CompletionRequest
        else:
            raise ValueError(f"Unsupported method: {self._method}")

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any], float]:
        """
        Run the serve deployment.

        Args:
            row: The row to run the serve deployment on.

        Returns:
            The response from the serve deployment.
        """
        request = self._prepare_request(row)
        t = time.perf_counter()

        output_data = await anext(
            getattr(self._dh, self._method).remote(self._request_type(**request))
        )
        time_taken = time.perf_counter() - t
        return request, output_data.model_dump(), time_taken

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Run the serve deployment.

        Args:
            batch: A list of rows to run the serve deployment on.
        """
        batch_uuid = uuid.uuid4()
        t = time.perf_counter()
        tasks = [asyncio.create_task(self.generate_async(row)) for row in batch]

        for resp in asyncio.as_completed(tasks):
            request, output, time_taken = await resp

            yield {
                "request_id": request["request_id"],
                self.IDX_IN_BATCH_COLUMN: request["idx_in_batch"],
                "batch_uuid": batch_uuid.hex,
                "time_taken_llm": time_taken,
                **output,
            }

        batch_time_taken = time.perf_counter() - t
        logger.info(
            "[LLM Batch - Serve Deployment] Elapsed time for batch %s with size %d: %s",
            batch_uuid.hex,
            len(batch),
            batch_time_taken,
        )


class ServeDeploymentStage(StatefulStage):
    fn: Type[StatefulStageUDF] = ServeDeploymentStageUDF
