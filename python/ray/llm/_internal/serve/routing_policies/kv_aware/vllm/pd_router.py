"""Router application for KV-aware prefill/decode disaggregation."""

import base64
import json
from typing import Any, Dict, List, Optional, Union

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from ray import serve
from ray.llm._internal.serve.core.configs.openai_api_models import ErrorResponse
from ray.llm._internal.serve.core.ingress.router import (
    _BODY_TRUNCATED_HEADER,
    LLMRouter,
    _parse_request_body,
    _with_request_options,
)
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DECODE_SELECTION_SVC_ENV,
    KV_TRANSFER_PARAMS_HEADER,
    PD_BACKEND_APP_NAME,
    PD_PREFILL_ENDPOINT_KEY,
    PREFILL_SELECTION_SVC_ENV,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.pd_prefill import (
    PDRequest,
    PrefillClient,
    PrefillRequest,
    PrefillResult,
    parse_pd_request,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
)


@serve._router_application
class LLMPDRouter(LLMRouter):
    """Select and run P, then select D. HAProxy sends the request to that D.

    P and D each have a KVTokenTracker with its own selection service, router
    config, lifecycle endpoint, and replica mesh. A session binds to a P
    replica and to a D replica separately.

    Only ZMQ crosses replicas: the router runs prefill on the selected P
    replica with a ZMQ call, and both engines report lifecycle events to their
    stage's tracker on this router.

    Truncated bodies are rejected. Other routes and bodies that fail validation
    go to a D replica, which serves them itself.
    """

    async def __init__(
        self,
        prefill_deployment: str,
        decode_deployment: str,
        prefill_config,
        decode_config,
    ):
        self._prefill = serve.get_deployment_handle(
            prefill_deployment, app_name=PD_BACKEND_APP_NAME
        )
        self._prefill_client = PrefillClient()
        # LLMRouter's _kv_token_tracker tracks the decode deployment.
        await super().__init__(
            serve.get_deployment_handle(
                decode_deployment, app_name=PD_BACKEND_APP_NAME
            ),
            decode_config,
            base_model_id=decode_config.model_id if decode_config.lora_config else None,
            selection_svc_env=DECODE_SELECTION_SVC_ENV,
        )
        self._prefill_tracker = self._build_kv_token_tracker(
            prefill_config, self._prefill, PREFILL_SELECTION_SVC_ENV
        )
        await self._prefill_tracker.start_lifecycle_receiver()
        self._prefill._init()

    async def route_request(
        self, request: Request
    ) -> Union[Dict[str, Any], JSONResponse]:
        if _BODY_TRUNCATED_HEADER in request.headers:
            raise HTTPException(
                413,
                "Request body exceeds the routing buffer. Raise "
                "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE.",
            )
        raw_body = await request.body()
        body = _parse_request_body(raw_body)
        path = request.headers.get(SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER, "")
        multiplexed_model_id = self._get_multiplexed_model_id(body)
        session_id = self._get_session_id(request)
        decode = self._get_handle(multiplexed_model_id, session_id)
        pd_request = parse_pd_request(path, body)
        if pd_request is None:
            # Other routes and invalid bodies go to D, which serves them itself.
            _, _, replica_id, _ = await self._pick_replica(decode)
            return self._build_decision(
                replica_id, self._build_request_headers(multiplexed_model_id)
            )
        # Tokenize the validated request (no second validation) on a copy, since
        # vLLM's renderer may adjust the request in place.
        token_ids = await self._tokenize(pd_request.model_copy())

        transfer_params = await self._run_prefill(
            request, pd_request, token_ids, multiplexed_model_id, session_id
        )
        if isinstance(transfer_params, ErrorResponse):
            # P's error (e.g. prompt too long) goes to the client unchanged.
            return JSONResponse(
                transfer_params.model_dump(),
                status_code=transfer_params.error.code or 400,
            )

        # D's KVAwareRouter reads the output-token cap off the request.
        _, _, replica_id, token_endpoint = await self._pick_replica(
            decode, pd_request, token_ids
        )
        headers = self._build_request_headers(
            multiplexed_model_id,
            replica_id=replica_id,
            token_endpoint=token_endpoint,
            request_token_ids=token_ids,
            messages=body.get("messages", []),
        )
        headers[KV_TRANSFER_PARAMS_HEADER] = base64.b64encode(
            json.dumps(transfer_params).encode()
        ).decode()
        return self._build_decision(replica_id, headers)

    async def _run_prefill(
        self,
        request: Request,
        pd_request: PDRequest,
        token_ids: List[int],
        multiplexed_model_id: str,
        session_id: Optional[str],
    ) -> PrefillResult:
        """Select a P replica, which books its prefill load, then run prefill
        there over ZMQ."""
        prefill = _with_request_options(self._prefill, multiplexed_model_id, session_id)
        selection = await self._choose_replica(prefill, pd_request, token_ids)
        # P's KVAwareRouter booked the selection under the request's id. P runs
        # the request, and reports its lifecycle, under the same id.
        request_id = selection._request_metadata.request_id
        endpoint = selection.replica_metadata.get(PD_PREFILL_ENDPOINT_KEY)
        prefill_request = PrefillRequest(
            path=request.headers.get(SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER, ""),
            body=await request.body(),
            token_ids=token_ids,
            request_id=request_id,
            lifecycle_owner=self._prefill_tracker.lifecycle_endpoint,
            multiplexed_model_id=multiplexed_model_id,
            headers=dict(request.headers),
        )
        try:
            if not endpoint:
                raise RuntimeError(
                    f"Prefill replica {selection.replica_id} does not serve "
                    "P/D prefill calls."
                )
            return await self._prefill_client.prefill(endpoint, prefill_request)
        except BaseException as e:
            # The engine may never report a prefill that failed or was
            # cancelled, so free its booking now. A later report is a no-op.
            self._prefill_tracker.release_request(request_id)
            if isinstance(e, (ConnectionError, TimeoutError)):
                # P was lost or timed out. route() returns RuntimeError as a
                # retryable 503.
                raise RuntimeError(f"Prefill on {endpoint} failed: {e!r}") from e
            raise

    def _build_decision(
        self, replica_id: str, headers: Dict[str, str]
    ) -> Dict[str, Any]:
        return {
            "application": PD_BACKEND_APP_NAME,
            "replica_id": replica_id,
            RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD: headers,
        }

    async def __del__(self) -> None:
        await self._prefill_client.close()
        await super().__del__()
