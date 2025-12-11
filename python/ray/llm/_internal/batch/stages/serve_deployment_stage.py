"""The stage that runs serve deployment."""

import asyncio
import logging
import time
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel

from ray import serve
from ray.exceptions import RayActorError, RayTaskError
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.serve.exceptions import BackPressureError, DeploymentUnavailableError

logger = logging.getLogger(__name__)

# Maximum prompt length to include in error messages for debuggability.
_MAX_PROMPT_LENGTH_IN_ERROR = 200

# Fatal errors that indicate the deployment/replica is dead or unavailable.
# These should always propagate immediately - continuing would be futile.
# NOTE: BackPressureError could benefit from retry logic in the future,
# but for now we treat it as fatal since retries require additional machinery.
_SERVE_FATAL_ERRORS = (
    RayActorError,  # Replica crashed (includes ActorDiedError, ActorUnavailableError)
    BackPressureError,  # System overloaded, requests being dropped
    DeploymentUnavailableError,  # Deployment failed to deploy
)


class ServeDeploymentStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        *,
        deployment_name: str,
        app_name: str,
        dtype_mapping: Dict[str, Type[Any]],
        should_continue_on_error: bool = False,
    ):
        """
        Initialize the ServeDeploymentStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            deployment_name: The name of the deployment.
            app_name: The name of the deployment app.
            dtype_mapping: The mapping of the request class name to the request class.
            should_continue_on_error: If True, continue processing when inference
                fails for a row instead of raising. Failed rows will have
                '__inference_error__' set to the error message.
        """
        super().__init__(data_column, expected_input_keys)
        self._dtype_mapping = dtype_mapping
        self.should_continue_on_error = should_continue_on_error

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
            A tuple of (decorated_request, dtype, method_name). dtype is the class of
            the request object and can be None if the serve deployment accepts a raw
            dict. method_name is the name of the method to invoke on the deployment.
        """
        method = row.get("method")
        dtype_name = row.get("dtype")

        dtype = None
        if dtype_name is not None:
            if not self._dtype_mapping or dtype_name not in self._dtype_mapping:
                raise ValueError(
                    f"{dtype_name} must be provided in "
                    "ServeDeploymentProcessorConfig's dtype_mapping."
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

    def _is_fatal_error(self, exc: Exception) -> bool:
        """Check if an exception is fatal (deployment/replica level failure).

        Fatal errors indicate the deployment or replica is dead/unavailable,
        so continuing to process more rows would be futile.

        Args:
            exc: The exception to check.

        Returns:
            True if the error is fatal and should always propagate.
        """
        # Direct fatal errors
        if isinstance(exc, _SERVE_FATAL_ERRORS):
            return True

        # RayTaskError wraps exceptions from remote calls. Check if the
        # underlying cause is a fatal error.
        if isinstance(exc, RayTaskError) and hasattr(exc, "cause"):
            if isinstance(exc.cause, _SERVE_FATAL_ERRORS):
                return True

        return False

    async def _generate_with_error_handling(
        self,
        row: Dict[str, Any],
        batch_uuid: uuid.UUID,
    ) -> Dict[str, Any]:
        """Generate output for a row, catching errors if should_continue_on_error is set.

        Args:
            row: The input row.
            batch_uuid: The batch UUID for logging.

        Returns:
            The output dict, with __inference_error__ set if an error occurred.
        """
        idx_in_batch = row[self.IDX_IN_BATCH_COLUMN]
        try:
            request, output, time_taken = await self.generate_async(row)

            return {
                "request_id": request["request_id"],
                self.IDX_IN_BATCH_COLUMN: request["idx_in_batch"],
                "batch_uuid": batch_uuid.hex,
                "time_taken": time_taken,
                "__inference_error__": None,
                **output,
            }
        except Exception as e:
            # Fatal errors always propagate - replica/deployment is dead
            if self._is_fatal_error(e):
                raise

            if not self.should_continue_on_error:
                raise

            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.warning(
                "[Serve Deployment] Inference failed for row %d in batch %s: %s",
                idx_in_batch,
                batch_uuid.hex,
                error_msg,
            )

            # Include request_kwargs snippet for debuggability
            request_kwargs = row.get("request_kwargs", {})
            prompt = str(request_kwargs)
            if len(prompt) > _MAX_PROMPT_LENGTH_IN_ERROR:
                prompt = prompt[:_MAX_PROMPT_LENGTH_IN_ERROR] + "...[truncated]"

            return {
                self.IDX_IN_BATCH_COLUMN: idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "__inference_error__": error_msg,
                "request_kwargs": prompt,
            }

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Run the serve deployment.

        Args:
            batch: A list of rows to run the serve deployment on.

        Yields:
            Dict[str, Any]: A dictionary containing the response from the serve
            deployment along with processing metadata.
        """
        batch_uuid = uuid.uuid4()
        t = time.perf_counter()

        if self.should_continue_on_error:
            # Use error-handling wrapper for each row
            tasks = [
                asyncio.create_task(self._generate_with_error_handling(row, batch_uuid))
                for row in batch
            ]

            for resp in asyncio.as_completed(tasks):
                result = await resp
                yield result
        else:
            # Original behavior: exceptions propagate immediately
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
            "request_kwargs": "The request_kwargs to construct the request.",
        }
