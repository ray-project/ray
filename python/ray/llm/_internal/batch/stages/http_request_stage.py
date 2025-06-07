"""HTTP Request Stage"""

import asyncio
import time
import traceback
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Type

import aiohttp
import aiohttp.web_exceptions
import numpy as np
from aiohttp.client_exceptions import ClientPayloadError

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF


class HttpRequestUDF(StatefulStageUDF):
    RETRYABLE_STATUS_CODES = [429, 408, 504, 502, 503]

    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        url: str,
        additional_header: Optional[Dict[str, Any]] = None,
        qps: Optional[int] = None,
        max_retries: int = 0,
        base_retry_wait_time_in_s: float = 1.0,
        session_factory: Optional[Callable[[], aiohttp.ClientSession]] = None,
    ):
        """
        Initialize the HttpRequestUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            url: The URL to send the HTTP request to.
            additional_header: The additional headers to send with the HTTP request.
            qps: The maximum number of requests per second.
            max_retries: The maximum number of retries per request in the event of failures. We retry with exponential backoff upto this specific maximum retries.
            base_retry_wait_time_in_s: The base retry wait time during exponential backoff.
            session_factory: Optional session factory to be used for initializing a client session.
        """
        super().__init__(data_column, expected_input_keys)
        self.url = url
        self.additional_header = additional_header or {}
        self.qps = qps
        self.max_retries = max_retries
        self.base_retry_wait_time_in_s = base_retry_wait_time_in_s
        self.session_factory = session_factory or aiohttp.ClientSession

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Send HTTP requests to the given URL.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows of the response of the HTTP request.
        """
        # preprocess to get request body for the given batch
        request_bodies = [None] * len(batch)
        for row in batch:
            # Normalize the row to a JSON body.
            json_body = {}
            for key, value in row["payload"].items():
                if isinstance(value, np.ndarray):
                    json_body[key] = value.tolist()
                else:
                    json_body[key] = value
            request_bodies[row[self.IDX_IN_BATCH_COLUMN]] = json_body

        async with self.session_factory() as session:
            start_time = time.time()
            request_count = 0
            pending_requests = []
            headers = {
                "Content-Type": "application/json",
                **self.additional_header,
            }

            # First send all requests based on QPS
            for row in batch:
                # Rate limit based on qps if specified
                if self.qps is not None:
                    request_count += 1
                    expected_time = request_count / self.qps
                    elapsed = time.time() - start_time
                    if elapsed < expected_time:
                        await asyncio.sleep(expected_time - elapsed)

                # self.IDX_IN_BATCH_COLUMN is the index of row in the batch
                json_body = request_bodies[row[self.IDX_IN_BATCH_COLUMN]]
                # Create request but don't await it yet
                request = session.post(
                    self.url,
                    headers=headers,
                    json=json_body,
                )
                pending_requests.append((row[self.IDX_IN_BATCH_COLUMN], request))

            # Now receive all responses
            for idx_in_batch_column, request in pending_requests:
                resp_json = None
                last_exception = None
                last_exception_traceback = None
                for retry_count in range(self.max_retries + 1):
                    if retry_count > 0:
                        json_body = request_bodies[idx_in_batch_column]
                        request = session.post(
                            self.url,
                            headers=headers,
                            json=json_body,
                        )
                    try:
                        async with await request as response:
                            status_code = response.status
                            # check status and see if it's retry worthy
                            if status_code in self.RETRYABLE_STATUS_CODES:
                                last_exception = aiohttp.web_exceptions.HTTPException(
                                    reason=response.reason
                                )
                                last_exception.status_code = status_code
                                wait_time = self.base_retry_wait_time_in_s * (
                                    2**retry_count
                                )
                                await asyncio.sleep(wait_time)
                                continue
                            resp_json = await response.json()
                            if self.IDX_IN_BATCH_COLUMN in resp_json:
                                raise ValueError(
                                    "The response of the HTTP request must not contain "
                                    f"the column {self.IDX_IN_BATCH_COLUMN}."
                                )
                        break
                    except (
                        asyncio.TimeoutError,
                        aiohttp.ClientConnectionError,
                        ClientPayloadError,
                    ) as e:
                        last_exception_traceback = traceback.format_exc()
                        last_exception = type(e).__name__
                        wait_time = self.base_retry_wait_time_in_s * (2**retry_count)
                        await asyncio.sleep(wait_time)
                        continue
                if not resp_json:
                    raise RuntimeError(
                        f"Reached maximum retries of {self.max_retries} for input row {batch[idx_in_batch_column]}. Previous Exception: {last_exception}. Full Traceback: \n{last_exception_traceback}"
                    )
                yield {
                    self.IDX_IN_BATCH_COLUMN: idx_in_batch_column,
                    "http_response": resp_json,
                }


class HttpRequestStage(StatefulStage):
    """
    A stage that sends HTTP requests.
    """

    fn: Type[StatefulStageUDF] = HttpRequestUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        return {
            "payload": "The payload to send to the HTTP request. "
            "It should be in JSON format."
        }
