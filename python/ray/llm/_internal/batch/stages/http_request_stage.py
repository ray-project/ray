"""HTTP Request Stage"""

import aiohttp
import asyncio
import time
import numpy as np
from typing import Any, Dict, AsyncIterator, Optional, List, Type

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF


class HttpRequestUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        url: str,
        additional_header: Optional[Dict[str, Any]] = None,
        qps: Optional[int] = None,
    ):
        """
        Initialize the HttpRequestUDF.

        Args:
            data_column: The data column name.
            url: The URL to send the HTTP request to.
            additional_header: The additional headers to send with the HTTP request.
            qps: The maximum number of requests per second.
        """
        super().__init__(data_column)
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
                for key, value in row["payload"].items():
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
                pending_requests.append((row[self.IDX_IN_BATCH_COLUMN], request))

            # Now receive all responses
            for idx_in_batch, request in pending_requests:
                async with await request as response:
                    resp_json = await response.json()
                    if self.IDX_IN_BATCH_COLUMN in resp_json:
                        raise ValueError(
                            "The response of the HTTP request must not contain "
                            f"the column {self.IDX_IN_BATCH_COLUMN}."
                        )
                    yield {
                        self.IDX_IN_BATCH_COLUMN: idx_in_batch,
                        "http_response": resp_json,
                    }

    @property
    def expected_input_keys(self) -> List[str]:
        return ["payload"]


class HttpRequestStage(StatefulStage):
    """
    A stage that sends HTTP requests.
    """

    fn: Type[StatefulStageUDF] = HttpRequestUDF
