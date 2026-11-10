"""Router application for KV-aware prefill/decode disaggregation."""

import asyncio
import base64
import contextlib
import json
from typing import Any, AsyncIterator, Dict, List, Optional, Union

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from ray import serve
from ray.exceptions import RayError
from ray.llm._internal.serve.core.configs.openai_api_models import ErrorResponse
from ray.llm._internal.serve.core.ingress.router import (
    _BODY_TRUNCATED_HEADER,
    LLMRouter,
    _parse_request_body,
    _with_request_options,
)
from ray.llm._internal.serve.core.protocol import RawRequestInfo
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DECODE_SELECTION_SVC_ENV,
    KV_TRANSFER_PARAMS_HEADER,
    PD_BACKEND_APP_NAME,
    PREFILL_SELECTION_SVC_ENV,
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.pd_prefill import (
    PDRequest,
    PrefillResult,
    parse_pd_request,
)
from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S,
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
)
from ray.serve._private.request_router.replica_wrapper import ReplicaSelection
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle


@serve._router_application
class LLMPDRouter(LLMRouter):
    """Select and run P, then select D. HAProxy sends the request to that D.

    P and D each have a KVTokenTracker with its own selection service, router
    config, lifecycle endpoint, and replica mesh. A session binds to a P
    replica and to a D replica separately.

    The router runs prefill on the selected P replica with a Serve handle call.
    Both engines report lifecycle events over ZMQ to their stage's tracker on
    this router.

    Truncated bodies are rejected. Other routes and bodies that fail validation
    go to a D replica, which serves them itself.
    """

    # Past HAProxy's routing timeout, nobody awaits the decision.
    _prefill_timeout_s: float = RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S

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
        there with a handle call."""
        prefill = _with_request_options(
            self._prefill, multiplexed_model_id, session_id
        ).prefill
        # The handle call carries this route request's id: P's KVAwareRouter
        # books the selection under it, and P runs the request, and reports its
        # lifecycle, under it.
        request_id = serve.context._get_serve_request_context().request_id
        try:
            # Selection is inside the timeout too: Serve retries a reserved
            # selection until a replica takes it.
            return await asyncio.wait_for(
                self._call_prefill(prefill, request, pd_request, token_ids),
                self._prefill_timeout_s,
            )
        except BaseException as e:
            # The engine may never report a prefill that failed or was
            # cancelled, so free its booking now. A later report is a no-op.
            self._prefill_tracker.release_request(request_id)
            if isinstance(e, (RayError, RayServeException, asyncio.TimeoutError)):
                # P was lost, failed the call, or timed out. route() returns
                # RuntimeError as a retryable 503.
                raise RuntimeError(
                    f"Prefill on {self._prefill.deployment_name} failed: {e!r}"
                ) from e
            raise

    async def _call_prefill(
        self,
        prefill: DeploymentHandle,
        request: Request,
        pd_request: PDRequest,
        token_ids: List[int],
    ) -> PrefillResult:
        """Run ``prefill`` on the P replica it selects, with a handle call."""
        async with self._reserve_prefill_replica(
            prefill, pd_request, token_ids
        ) as selection:
            response = prefill.dispatch(
                selection,
                pd_request,
                RawRequestInfo(headers=dict(request.headers)),
                token_ids,
                lifecycle_owner=self._prefill_tracker.lifecycle_endpoint,
            )
            try:
                # Awaited before the selection exits, which Serve requires of a
                # dispatched call.
                return await response
            except asyncio.CancelledError:
                # Timed out or cancelled: stop the prefill nobody awaits.
                response.cancel()
                raise

    @contextlib.asynccontextmanager
    async def _reserve_prefill_replica(
        self,
        prefill: DeploymentHandle,
        pd_request: PDRequest,
        token_ids: List[int],
    ) -> AsyncIterator[ReplicaSelection]:
        """Select a P replica with P's KVAwareRouter, which books its prefill
        load, and reserve a slot on it for the handle call."""
        async with prefill.choose_replica(
            pd_request, **{REQUEST_TOKEN_IDS_KWARG: token_ids}
        ) as selection:
            yield selection

    def _build_decision(
        self, replica_id: str, headers: Dict[str, str]
    ) -> Dict[str, Any]:
        return {
            "application": PD_BACKEND_APP_NAME,
            "replica_id": replica_id,
            RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD: headers,
        }
