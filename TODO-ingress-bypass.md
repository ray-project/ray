# Ingress Bypass Hardening

## Observability

Wrap vLLM's FastAPI app in ASGI middleware in
`LLMServer._register_direct_ingress_app()` before `set_asgi_app`. Intercept
`http.response.start` (status) and first non-empty `http.response.body`
(TTFT). Emit the same Serve metrics: request count, latency histogram, status
codes, TTFT. Runs in-process, no extra hop.

Generate a trace ID in Lua, send it to `/internal/route`, inject it as a
header on the forwarded request. Correlates HAProxy logs, ingress routing
logs, and vLLM request logs.

Set `txn.lua_route_failed` on every Lua failure path and include it in the
HAProxy `log-format`. Right now all failures are silent fallthroughs.

## Fault Tolerance

Lua hardcodes `router_servers[0]`, so one dead ingress pod breaks all
custom-routed streaming. Emit all router servers into the Lua table and
iterate on failure.

Drop the Lua socket timeout from 5s to 500ms. `choose_replicas()` returns in
under a millisecond; 5s just blocks the HAProxy thread on a dead router.

Add `retry-on conn-failure empty-response 0rtt-rejected` and
`option redispatch` to the custom-routed backend. A replica dying
pre-response is currently a bare 502.

Draining replicas reject Serve requests but still pass `/health`, so HAProxy
keeps sending bypass traffic. Fail the health check during drain.

## Routing Accuracy

Bypassed requests don't update `MetricsManager._num_ongoing_requests`, so
`choose_replicas()` sees replicas as idle and makes increasingly wrong
placement decisions as bypass traffic grows.

The ASGI middleware (same one added for metrics above) calls
`inc_num_ongoing_requests` / `dec_num_ongoing_requests` on the destination
replica when the request arrives and completes. Same counter the Serve path
uses. The router's existing probe of `get_num_ongoing_requests()` picks it up
automatically. No new RPCs.

## Cleanup

- [x] Remove `_DirectIngressRegistry` actor and all `_register_direct_ingress_endpoint`
      call sites -- redundant with `RunningReplicaInfo.direct_ingress_http_port`
- [x] Add `router` field to protobuf `DeploymentArgs` so it survives controller
      checkpoint/recovery; removed `name_to_router_deployment` side-channel
- [x] Remove `_summarize_target*` / `_summarize_fallback_targets` from
      `controller.py` and `_summarize_target*` / `_summarize_backend_config` from
      `haproxy.py` (leftover debug helpers from bisect iterations)
