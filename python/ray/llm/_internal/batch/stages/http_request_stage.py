"""HTTP Request Stage"""

import asyncio
import json
import time
import traceback
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Type

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
    MULTIPART_CONTENT_TYPE = "multipart/form-data"
    SUPPORTED_CONTENT_TYPES = (JSON_CONTENT_TYPE, MULTIPART_CONTENT_TYPE)

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
        content_type: Optional[str] = None,
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
            content_type: The content type of the request body. Either
                "application/json" (default), in which case each row's "payload"
                is serialized to a JSON body, or "multipart/form-data", in which
                case each row's "payload" is sent as a multipart form. The latter
                is required to upload files (e.g. OpenAI's audio transcription
                API). See ``_build_form_data`` for the expected payload schema.
        """
        super().__init__(data_column, expected_input_keys)
        self.url = url
        self.additional_header = additional_header or {}
        self.qps = qps
        self.max_retries = max_retries
        self.base_retry_wait_time_in_s = base_retry_wait_time_in_s
        self.session_factory = session_factory or aiohttp.ClientSession

        self.content_type = content_type or self.JSON_CONTENT_TYPE
        if self.content_type not in self.SUPPORTED_CONTENT_TYPES:
            raise ValueError(
                f"Unsupported content_type {self.content_type!r}. Supported "
                f"content types are {self.SUPPORTED_CONTENT_TYPES}."
            )
        self.is_multipart = self.content_type == self.MULTIPART_CONTENT_TYPE
        # For multipart requests, aiohttp sets the "Content-Type" header
        # (including the boundary) from the FormData object, so we must not set
        # it ourselves. We also drop any user-supplied "Content-Type" from
        # additional_header: aiohttp only auto-generates the multipart boundary
        # when no Content-Type is set, so leaving one in would break uploads.
        if self.is_multipart:
            self.headers = {
                k: v
                for k, v in self.additional_header.items()
                if k.lower() != "content-type"
            }
        else:
            self.headers = {
                "Content-Type": self.JSON_CONTENT_TYPE,
                **self.additional_header,
            }

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

    def _build_request_body(self, payload: Dict[str, Any]) -> Any:
        """Build the ``data`` argument for a single request from its payload."""
        if self.is_multipart:
            if not isinstance(payload, dict):
                raise TypeError(
                    'For multipart/form-data, each row\'s "payload" must be a '
                    f"dict, but got {type(payload).__name__}: {payload!r}"
                )
            # A FormData object is consumed when the request is sent, so a fresh
            # one must be built for every (re)try.
            return self._build_form_data(payload)
        return json.dumps(payload, cls=NumpyEncoder)

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Send HTTP requests to the given URL.

        Args:
            batch: A list of rows to send.

        Yields:
            A generator of rows of the response of the HTTP request.
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
            headers = self.headers

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
                body = self._build_request_body(payloads[row[self.IDX_IN_BATCH_COLUMN]])
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
                        body = self._build_request_body(payloads[idx_in_batch_column])
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
