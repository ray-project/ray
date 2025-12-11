"""The stage that runs serve deployment."""

import asyncio
import logging
import time
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel

from ray import serve
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)

logger = logging.getLogger(__name__)


class ServeDeploymentStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        *,
        deployment_name: str,
        app_name: str,
        dtype_mapping: Dict[str, Type[Any]],
    ):
        """
        Initialize the ServeDeploymentStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            deployment_name: The name of the deployment.
            app_name: The name of the deployment app.
            dtype_mapping: The mapping of the request class name to the request class.
        """
        super().__init__(data_column, expected_input_keys)
        self._dtype_mapping = dtype_mapping

        # Using stream=True as LLM serve deployments return async generators.
        # TODO (Kourosh): Generalize this to support non-streaming deployments.
        self._dh = serve.get_deployment_handle(deployment_name, app_name).options(
            stream=True
        )
        self.request_id = 0

    def _prepare_request(
        self, row: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Optional[Type[Any]], str]:
        """
        Decorate the request with metadata related to the batch.

        Args:
            row: The row.

        Returns:
            A tuple of (decorated_request, dtype, method_name). dtype is the class of the request object and
            can be None if the serve deployment accepts a raw dict. method_name is the name of the method to
            invoke on the serve deployment.
        """
        method = row.get("method")
        dtype_name = row.get("dtype")

        dtype = None
        if dtype_name is not None:
            if not self._dtype_mapping or dtype_name not in self._dtype_mapping:
                raise ValueError(
                    f"{dtype_name} must be provided in ServeDeploymentProcessorConfig's dtype_mapping."
                )
            dtype = self._dtype_mapping[dtype_name]

        request_kwargs = row.pop("request_kwargs")
        request = {
            "request_id": str(self.request_id),
            "idx_in_batch": row[self.IDX_IN_BATCH_COLUMN],
            **request_kwargs,
        }
        self.request_id += 1

        return request, dtype, method

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
        request, dtype, method = self._prepare_request(row)
        request_obj = dtype(**request) if dtype else request

        if getattr(self._dh, method) is None:
            raise ValueError(f"Method {method} not found in the serve deployment.")

        t = time.perf_counter()
        # Directly using anext() requires python3.10 and above
        output_data = await getattr(self._dh, method).remote(request_obj).__anext__()
        time_taken = time.perf_counter() - t

        # Convert the output data to a dict if it is a Pydantic model.
        if isinstance(output_data, BaseModel):
            output_data = output_data.model_dump()

        return request, output_data, time_taken

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Run the serve deployment.

        Args:
            batch: A list of rows to run the serve deployment on.

        Yields:
            Dict[str, Any]: A dictionary containing the response from the serve deployment
            along with processing metadata.
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
                "time_taken": time_taken,
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

    def get_required_input_keys(self) -> Dict[str, str]:
        return {
            "method": "Name of the method to invoke on the serve deployment.",
            "request_kwargs": "The request_kwargs to construct the request to the serve deployment.",
        }
