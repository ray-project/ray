"""HTTP Request Stage"""

import aiohttp
import asyncio
import time
import numpy as np
from typing import Any, Dict, AsyncIterator, Optional, List

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF


class HttpRequestUDF(StatefulStageUDF):
    def __init__(
        self,
        input_column: str,
        output_column: str,
        carry_over: bool,
        url: str,
        additional_header: Optional[Dict[str, Any]] = None,
        qps: Optional[int] = None,
    ):
        """
        Initialize the HttpRequestUDF.

        Args:
            input_column: The input column name.
            output_column: The output column name.
            carry_over: Whether to carry over the input column to the output column.
            url: The URL to send the HTTP request to.
            additional_header: The additional headers to send with the HTTP request.
            qps: The maximum number of requests per second.
        """
        super().__init__(input_column, output_column, carry_over)
        self.url = url
        self.additional_header = additional_header or {}
        self.qps = qps

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Send HTTP requests to the given URL.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows of the response of the HTTP request.
        """
        async with aiohttp.ClientSession() as session:
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

                # Normalize the row to a JSON body.
                json_body = {}
                for key, value in row.items():
                    if isinstance(value, np.ndarray):
                        json_body[key] = value.tolist()
                    else:
                        json_body[key] = value

                # Create request but don't await it yet
                request = session.post(
                    self.url,
                    headers=headers,
                    json=json_body,
                )
                pending_requests.append(request)

            # Now receive all responses
            for request in pending_requests:
                async with await request as response:
                    yield await response.json()


class HttpRequestStage(StatefulStage):
    """
    A stage that sends HTTP requests.
    """

    fn: StatefulStageUDF = HttpRequestUDF
    fn_constructor_kwargs: Dict[str, Any]
    map_batches_kwargs: Dict[str, Any] = dict(
        concurrency=1,
    )
