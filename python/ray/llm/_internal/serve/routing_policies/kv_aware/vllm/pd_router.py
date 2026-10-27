"""Router application for KV-aware prefill/decode disaggregation."""

import asyncio
import base64
import json
from typing import Any, Dict, Optional, Union

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from ray import serve
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    ErrorResponse,
)
from ray.llm._internal.serve.core.ingress.router import (
    _BODY_TRUNCATED_HEADER,
    LLMRouter,
    _parse_request_body,
)
from ray.llm._internal.serve.core.ingress.utils import _sanitize_chat_completion_request
from ray.llm._internal.serve.core.protocol import RawRequestInfo
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DECODE_SELECTION_SVC_ENV,
    KV_TRANSFER_PARAMS_HEADER,
    PD_BACKEND_APP_NAME,
    PD_ROUTER_APP_NAME,
    PREFILL_SELECTION_SVC_ENV,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    build_kv_token_tracker,
    get_llm_router_handle,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
)

# P/D covers only chat and completions: PDDecodeServer and the KV connector
# backend handle only these request types. Other routes go to D, which serves
# them without P/D.
_REQUEST_TYPES = {
    "/v1/chat/completions": ChatCompletionRequest,
    "/v1/completions": CompletionRequest,
}


def _parse_pd_request(
    path: str, body: Optional[Dict[str, Any]]
) -> Optional[Union[ChatCompletionRequest, CompletionRequest]]:
    """The typed request P runs, or None when D serves the request alone."""
    request_cls = _REQUEST_TYPES.get(path.split("?", 1)[0])
    if request_cls is None:
        return None
    try:
        request = request_cls.model_validate(body)
    except ValidationError:
        return None
    if isinstance(request, ChatCompletionRequest):
        request = _sanitize_chat_completion_request(request)
    return request


@serve._router_application
class LLMPDRouter(LLMRouter):
    """Select and run P, then select D. HAProxy sends the request to that D.

    P and D each have a KVTokenTracker with its own selection service.
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
        app_name = serve.get_replica_context().app_name
        if app_name != PD_ROUTER_APP_NAME:
            # P/D engines send lifecycle events to the router app by this name.
            raise ValueError(
                f"Deploy the P/D router application as {PD_ROUTER_APP_NAME!r} "
                f"and its backend as {PD_BACKEND_APP_NAME!r}; got {app_name!r}."
            )
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
        self._prefill_tracker = build_kv_token_tracker(
            prefill_config, self._prefill.deployment_id, PREFILL_SELECTION_SVC_ENV
        )
        self._prefill_tracker.start_replica_broadcast(
            get_llm_router_handle(PD_ROUTER_APP_NAME), self._prefill.deployment_id
        )

    async def route_request(
        self, request: Request
    ) -> Union[Dict[str, Any], JSONResponse]:
        if _BODY_TRUNCATED_HEADER in request.headers:
            raise HTTPException(
                413,
                "Request body exceeds the routing buffer. Raise "
                "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE.",
            )
        body = _parse_request_body(await request.body())
        path = request.headers.get(SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER, "")
        multiplexed_model_id = self._get_multiplexed_model_id(body)
        decode = self._get_handle(multiplexed_model_id)
        pd_request = _parse_pd_request(path, body)
        if pd_request is None:
            # D validates the request and serves it (see _REQUEST_TYPES).
            _, _, replica_id, _ = await self._pick_replica(decode)
            return self._build_decision(
                replica_id, self._build_request_headers(multiplexed_model_id)
            )
        # Tokenize the validated request (no second validation) on a copy, since
        # vLLM's renderer may adjust the request in place.
        token_ids = await self._tokenize(pd_request.model_copy())

        prefill = self._prefill
        if multiplexed_model_id:
            prefill = prefill.options(multiplexed_model_id=multiplexed_model_id)
        # P's KVAwareRouter selects the replica from request_token_ids, and P
        # reuses them instead of tokenizing again.
        response = prefill.prefill.remote(
            pd_request,
            RawRequestInfo.from_starlette_request(request),
            request_token_ids=token_ids,
        )
        try:
            transfer_params = await response
        except asyncio.CancelledError:
            response.cancel()
            raise
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

    def _build_decision(
        self, replica_id: str, headers: Dict[str, str]
    ) -> Dict[str, Any]:
        return {
            "application": PD_BACKEND_APP_NAME,
            "replica_id": replica_id,
            RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD: headers,
        }
