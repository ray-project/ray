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

        # Whether a request is sent as JSON or multipart/form-data is detected
        # per row from its payload (see ``_is_multipart_payload``), so we
        # precompute the headers for both cases here.
        # For JSON requests we default "Content-Type" to application/json (a
        # user-supplied value in additional_header still wins).
        self.json_headers = {
            "Content-Type": self.JSON_CONTENT_TYPE,
            **self.additional_header,
        }
        # For multipart requests, aiohttp sets the "Content-Type" header
        # (including the boundary) from the FormData object, so we must not set
        # it ourselves. We also drop any user-supplied "Content-Type" from
        # additional_header: aiohttp only auto-generates the multipart boundary
        # when no Content-Type is set, so leaving one in would break uploads.
        self.multipart_headers = {
            k: v
            for k, v in self.additional_header.items()
            if k.lower() != "content-type"
        }

    @staticmethod
    def _is_multipart_payload(payload: Any) -> bool:
        """Whether a payload should be sent as ``multipart/form-data``.

        A payload is treated as a file upload (and thus sent as multipart) when
        it is a dict with at least one "file-like" value: ``bytes`` /
        ``bytearray``, or a nested dict with a ``"content"`` key (see
        ``_build_form_data`` for the field schema). Everything else is sent as a
        JSON body. This mirrors how ``requests``/``httpx`` switch to a multipart
        encoding when files are passed, so callers never have to select a
        content type by hand.
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
        """Build a multipart ``aiohttp.FormData`` body from a payload row.

        Each key/value pair in ``payload`` becomes a form field:

        - A ``dict`` with a ``"content"`` key is treated as a file field. Its
          optional ``"filename"`` (defaults to the field name) and
          ``"content_type"`` keys control the multipart part metadata, e.g.
          ``{"file": {"content": b"...", "filename": "audio.mp3",
          "content_type": "audio/mpeg"}}``.
        - ``bytes``/``bytearray`` values are treated as a file field whose
          filename defaults to the field name.
        - ``str`` values are sent as-is. Any other value is JSON-encoded so that
          e.g. numpy scalars and nested structures are handled consistently.
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
        """Build the ``(data, headers)`` for a single request from its payload.

        Multipart payloads (file uploads) are encoded as an ``aiohttp.FormData``
        body and use the multipart headers; everything else is serialized to a
        JSON string with the JSON headers. A fresh body is built on every call
        because an ``aiohttp.FormData`` object is consumed when the request is
        sent and cannot be reused on retries.
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
        # Keep the raw payload per row so the request body can be (re)built on
        # demand. This is required for multipart requests because an aiohttp
        # FormData object is consumed once it is sent and cannot be reused on
        # retries.
        # Keyed by IDX_IN_BATCH_COLUMN rather than a list: ``batch`` only
        # contains the normal (non-error) rows, but IDX_IN_BATCH_COLUMN is the
        # row's index in the original, full batch, so it can exceed len(batch).
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
                    # Look up the payload by IDX_IN_BATCH_COLUMN: ``batch`` only
                    # holds the normal rows, but idx_in_batch_column is the index
                    # into the original full batch, so ``batch[...]`` could be
                    # out of range (or the wrong row) when the batch has error
                    # rows.
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
