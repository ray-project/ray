import asyncio
from typing import AsyncGenerator, Optional

from ray.exceptions import RayTaskError, TaskCancelledError

from ray.llm._internal.serve.observability.logging import get_logger

from ray.llm._internal.serve.observability.metrics.utils import (
    InstrumentTokenAsyncGenerator,
)
from ray.llm._internal.serve.deployments.utils.metrics_utils import Metrics
from ray.llm._internal.serve.configs.server_models import LLMRawResponse
from ray.llm._internal.serve.deployments.utils.server_utils import (
    get_response_for_error,
    get_serve_request_id,
)

logger = get_logger(__name__)


class StreamingErrorHandler:
    """Handle errors and finalizers for an LLMRawResponse stream.

    This class:
    1. Tracks request level metrics for the response stream
    2. Handles errors in the router level code for the response stream
    """

    def __init__(
        self,
        metrics: Optional[Metrics] = None,
    ):
        self.metrics = metrics or Metrics()

    @InstrumentTokenAsyncGenerator("router_get_response_stream")
    async def handle_failure(
        self,
        model: str,
        async_iterator: AsyncGenerator[LLMRawResponse, None],
    ):
        req_id = get_serve_request_id()
        context = {
            "model_id": model,
        }

        self.metrics.record_request(**context)

        is_first_token = True
        try:
            async for response in async_iterator:
                # First, yield the streamed response back
                yield response

                # Subsequently emit telemetry
                if is_first_token:
                    self.metrics.record_input_tokens(
                        response.num_input_tokens, **context
                    )
                    is_first_token = False

                self.metrics.record_streamed_response(response, **context)

        except asyncio.CancelledError:
            # NOTE: We just log cancellation and re-throw it immediately to interrupt
            #       request handling
            logger.warning(f"Request {req_id} has been cancelled")
            raise

        except RayTaskError as rte:
            if isinstance(rte.cause, TaskCancelledError):
                # NOTE: In cases of user-originated cancellation Ray Serve proxy will cancel
                #       downstream tasks recursively (using `ray.cancel`) leading to streaming
                #       ops resulting in TaskCancelledError.
                #
                #       From the application perspective this is no different from asyncio.CancelledError,
                #       therefore we just log cancellation and re-throw asyncio.CancelledError instead
                #       to facilitate proper clean up and avoid polluting our telemetry
                logger.warning(
                    f"Request {req_id} has been cancelled (Ray streaming generator task cancelled)"
                )
                raise asyncio.CancelledError() from rte

            yield get_response_for_error(rte, request_id=req_id)

        except Exception as e:
            logger.error(
                f"Failed while streaming back a response for request ({req_id}): {repr(e)}",
                exc_info=e,
            )

            self.metrics.record_failure(**context)

            yield get_response_for_error(e, request_id=req_id)
            # DO NOT RAISE.
            # We do not raise here because that would cause a disconnection for streaming.
