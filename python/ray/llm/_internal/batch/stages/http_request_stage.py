"""HTTP Request Stage"""

import asyncio
import json
import time
import traceback
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple, Type

import aiohttp
import aiohttp.web_exceptions
import numpy as np
from aiohttp.client_exceptions import ClientPayloadError

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return super().default(obj)


class HttpRequestUDF(StatefulStageUDF):
    RETRYABLE_STATUS_CODES = [429, 408, 504, 502, 503]

    JSON_CONTENT_TYPE = "application/json"

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

        self.json_headers = {
            "Content-Type": self.JSON_CONTENT_TYPE,
            **self.additional_header,
        }
        # Drop user Content-Type so aiohttp creates a matching multipart boundary
        # when it serializes FormData.
        self.multipart_headers = {
            k: v
            for k, v in self.additional_header.items()
            if k.lower() != "content-type"
        }

    @staticmethod
    def _is_multipart_payload(payload: Any) -> bool:
        """Return whether a payload should use multipart encoding.

        A dict is multipart when it contains bytes or a mapping with ``content``.
        """
        if not isinstance(payload, dict):
            return False
        return any(
            isinstance(value, (bytes, bytearray))
            or (isinstance(value, dict) and "content" in value)
            for value in payload.values()
        )

    @staticmethod
    def _build_form_data(payload: Dict[str, Any]) -> "aiohttp.FormData":
        """Build multipart form data from a payload.

        Bytes and ``content`` mappings become file fields; other values are form fields.
        """
        form = aiohttp.FormData()
        for key, value in payload.items():
            if isinstance(value, dict) and "content" in value:
                form.add_field(
                    key,
                    value["content"],
                    filename=value.get("filename", key),
                    content_type=value.get("content_type"),
                )
            elif isinstance(value, (bytes, bytearray)):
                form.add_field(key, value, filename=key)
            elif isinstance(value, str):
                form.add_field(key, value)
            else:
                form.add_field(key, json.dumps(value, cls=NumpyEncoder))
        return form

    def _build_request(self, payload: Any) -> Tuple[Any, Dict[str, Any]]:
        """Build a fresh request body and matching headers.

        FormData is consumed by a request, so retries need a fresh body.
        """
        if self._is_multipart_payload(payload):
            return self._build_form_data(payload), self.multipart_headers
        return json.dumps(payload, cls=NumpyEncoder), self.json_headers

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Send HTTP requests to the given URL.

        Args:
            batch: A list of rows to send.

        Yields:
            Dict[str, Any]: A generator of rows of the response of the HTTP request.
        """
        # Use original batch indexes because error rows are excluded before this UDF.
        payloads = {}
        for row in batch:
            payloads[row[self.IDX_IN_BATCH_COLUMN]] = row["payload"]

        async with self.session_factory() as session:
            start_time = time.time()
            request_count = 0
            pending_requests = []

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
                body, headers = self._build_request(
                    payloads[row[self.IDX_IN_BATCH_COLUMN]]
                )
                # Create request but don't await it yet
                request = session.post(
                    self.url,
                    headers=headers,
                    data=body,
                )
                pending_requests.append((row[self.IDX_IN_BATCH_COLUMN], request))

            # Now receive all responses
            for idx_in_batch_column, request in pending_requests:
                resp_json = None
                last_exception = None
                last_exception_traceback = None
                for retry_count in range(self.max_retries + 1):
                    if retry_count > 0:
                        body, headers = self._build_request(
                            payloads[idx_in_batch_column]
                        )
                        request = session.post(
                            self.url,
                            headers=headers,
                            data=body,
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
                        f"Reached maximum retries of {self.max_retries} for input row {payloads[idx_in_batch_column]}. Previous Exception: {last_exception}. Full Traceback: \n{last_exception_traceback}"
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
