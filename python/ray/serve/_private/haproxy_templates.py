HAPROXY_HEALTHZ_RULES_TEMPLATE = """    # Health check endpoint
    acl healthcheck path -i {{ config.health_check_endpoint }}
    # Suppress logging for health checks
    http-request set-log-level silent if healthcheck
{%- if not health_info.healthy %}
    # Override: force health checks to fail (used by drain/disable)
    http-request return status {{ health_info.status }} content-type text/plain string "{{ health_info.health_message }}" if healthcheck
{%- elif backends %}
    # 200 if any backend has at least one server UP
{%-   for backend in backends %}
    acl backend_{{ backend.name or 'unknown' }}_server_up nbsrv({{ backend.name or 'unknown' }}) ge 1
{%-   endfor %}
    # Any backend with a server UP passes the health check (OR logic)
{%-   for backend in backends %}
    http-request return status {{ health_info.status }} content-type text/plain string "{{ health_info.health_message }}" if healthcheck backend_{{ backend.name or 'unknown' }}_server_up
{%-   endfor %}
    http-request return status 503 content-type text/plain string "Service Unavailable" if healthcheck
{%- endif %}
"""

# Same shape as HAPROXY_HEALTHZ_RULES_TEMPLATE, but emits gRPC trailers-only
# responses. The OK / UNAVAILABLE distinction maps to grpc-status 0 / 14
# (gRPC clients map HTTP 503 to UNAVAILABLE, which is why every shape here
# uses HTTP 200 and signals state through grpc-status).
HAPROXY_GRPC_HEALTHZ_RULES_TEMPLATE = """    # Health check endpoint (gRPC `Healthz`)
    acl is_healthz path /ray.serve.RayServeAPIService/Healthz
    # Suppress logging for health checks
    http-request set-log-level silent if is_healthz
{%- if not health_info.healthy %}
    # Override: force health checks to fail (used by drain/disable)
    http-request return status 200 content-type application/grpc hdr grpc-status 14 hdr grpc-message "{{ health_info.health_message }}" if is_healthz
{%- elif backends %}
    # OK if any backend has at least one server UP
{%-   for backend in backends %}
    acl backend_{{ backend.name or 'unknown' }}_server_up nbsrv({{ backend.name or 'unknown' }}) ge 1
{%-   endfor %}
    # Any backend with a server UP passes the health check (OR logic)
{%-   for backend in backends %}
    http-request return status 200 content-type application/grpc hdr grpc-status 0 if is_healthz backend_{{ backend.name or 'unknown' }}_server_up
{%-   endfor %}
    http-request return status 200 content-type application/grpc hdr grpc-status 14 hdr grpc-message "Service Unavailable" if is_healthz
{%- endif %}
"""

HAPROXY_CONFIG_TEMPLATE = """global
    log {{ config.log_target }} local0 debug
    stats socket {{ config.socket_path }} mode 666 level admin expose-fd listeners
    stats timeout 30s
    maxconn {{ config.maxconn }}
    nbthread {{ config.nbthread }}
    {%- if has_ingress_request_router %}
    lua-load-per-thread {{ ingress_request_router_lua_path }}
    {%- endif %}
    {%- if has_ingress_request_router and ingress_request_router_forward_body %}
    tune.bufsize {{ ingress_request_router_bufsize }}
    {%- else %}
    tune.bufsize {{ config.bufsize }}
    {%- endif %}
    {%- if config.enable_hap_optimization %}
    server-state-base {{ config.server_state_base }}
    server-state-file {{ config.server_state_file }}
    {%- endif %}
    {%- if config.hard_stop_after_s is not none %}
    hard-stop-after {{ config.hard_stop_after_s }}s
    {%- endif %}
    {%- if config.grpc_enabled %}
    tune.h2.max-frame-size {{ config.h2_max_frame_size }}
    tune.h2.be.initial-window-size {{config.h2_be_initial_window_size}}
    tune.h2.be.max-concurrent-streams {{config.h2_be_max_concurrent_streams}}
    tune.h2.fe.initial-window-size {{config.h2_fe_initial_window_size}}
    tune.h2.fe.max-concurrent-streams {{config.h2_fe_max_concurrent_streams}}
    {%- endif %}
defaults
    mode http
    option log-health-checks
    {% if config.timeout_connect_s is not none %}timeout connect {{ config.timeout_connect_s }}s{% endif %}
    {% if config.timeout_client_s is not none %}timeout client {{ config.timeout_client_s }}s{% endif %}
    {% if config.timeout_server_s is not none %}timeout server {{ config.timeout_server_s }}s{% endif %}
    {% if config.timeout_http_request_s is not none %}timeout http-request {{ config.timeout_http_request_s }}s{% endif %}
    {% if config.timeout_http_keep_alive_s is not none %}timeout http-keep-alive {{ config.timeout_http_keep_alive_s }}s{% endif %}
    {% if config.timeout_queue_s is not none %}timeout queue {{ config.timeout_queue_s }}s{% endif %}
    log global
    option httplog
    option abortonclose
    option splice-request
    option splice-response
    {%- if config.tcp_nodelay %}
    # Set TCP_NODELAY on all connections
    option http-no-delay
    {%- endif %}
    {%- if config.enable_hap_optimization %}
    option idle-close-on-response
    {%- endif %}
    # Normalize 502 and 504 errors to 500 per Serve's default behavior
    {%- if config.error_file_path %}
    errorfile 502 {{ config.error_file_path }}
    errorfile 504 {{ config.error_file_path }}
    {%- endif %}
    {%- if config.enable_hap_optimization %}
    load-server-state-from-file global
    {%- endif %}
    balance {{ config.balance_algorithm }}
frontend prometheus
    bind :{{ config.metrics_port }}
    mode http
    http-request use-service prometheus-exporter if { path {{ config.metrics_uri }} }
    no log
frontend http_frontend
    bind {{ config.frontend_host }}:{{ config.frontend_port }}
    {%- if ingress_request_router_metrics_enabled and has_ingress_request_router %}
    log global
    # Per-request metrics for the ingress request router. Goes only to the
    # rfc5424 target below; the inherited rfc3164 targets do not include the
    # SD section, so their byte stream is unchanged.
    log {{ metrics_socket_path }} len 8192 format rfc5424 local1 info
    log-format-sd "%{+Q,+E}o [serve@1 app=%[var(txn.ingress_request_router_app)] intended=%[var(txn.ingress_request_router_target)] actual=%s router_latency_us=%[var(txn.ingress_request_router_latency_us)] body_truncated_full_length=%[var(txn.ingress_request_router_truncated_full_length)] via_router=%[var(txn.via_ingress_request_router)] failed=%[var(txn.ingress_request_router_failed)]]"
    {%- endif %}
{{ healthz_rules|safe }}
    # Routes endpoint
    acl routes path -i /-/routes
    http-request return status {{ route_info.status }} content-type {{ route_info.routes_content_type }} string "{{ route_info.routes_message }}" if routes

    {%- if config.inject_process_id_header and config.reload_id %}
    # Inject unique reload ID as header to track which HAProxy instance handled the request (testing only)
    http-request set-header x-haproxy-reload-id {{ config.reload_id }}
    {%- endif %}
    # Per-backend path ACLs (used for both ingress-request-router dispatch
    # and static use_backend selection below).
{%- for backend in backends %}
    acl is_{{ backend.name or 'unknown' }} path_beg {{ '/' if not backend.path_prefix or backend.path_prefix == '/' else backend.path_prefix ~ '/' }}
    acl is_{{ backend.name or 'unknown' }} path {{ backend.path_prefix or '/' }}
{%- endfor %}
    {%- if has_ingress_request_router %}
    # Set txn.ingress_request_router_app to the first matching router-bearing
    # backend. Backends are sorted longest-prefix-first, and the !found guard
    # ensures only the longest match wins.
    {%- for backend in backends %}
    {%- if backend.ingress_request_router_servers %}
    http-request set-var(txn.ingress_request_router_app) str({{ backend.name or 'unknown' }}) if is_{{ backend.name or 'unknown' }} !{ var(txn.ingress_request_router_app) -m found }
    {%- endif %}
    {%- endfor %}
    acl has_ingress_request_router_app var(txn.ingress_request_router_app) -m found
    {%- if ingress_request_router_forward_body %}
    http-request wait-for-body time {{ ingress_request_router_timeout_s }}s if METH_POST has_ingress_request_router_app
    {%- endif %}
    http-request lua.route_via_ingress_request_router if METH_POST has_ingress_request_router_app
    # Fail loudly when the Lua dispatch did not pick a replica. Must appear
    # before the use_backend rules below so the request never falls back to
    # the primary backend (which would be a silent bypass of the configured
    # router policy).
    http-request return status 503 content-type text/plain lf-string "Ingress request router failed: %[var(txn.ingress_request_router_failed)]" hdr X-Serve-Reason %[var(txn.ingress_request_router_failed)] if { var(txn.ingress_request_router_failed) -m found }
    {%- endif %}
    # Static routing based on path prefixes in decreasing length then alphabetical order
{%- for backend in backends %}
    {%- if has_ingress_request_router and backend.ingress_request_router_servers %}
    use_backend {{ backend.name or 'unknown' }}-via-ingress-request-router if is_{{ backend.name or 'unknown' }} { var(txn.via_ingress_request_router) -m found }
    {%- endif %}
    use_backend {{ backend.name or 'unknown' }} if is_{{ backend.name or 'unknown' }}
{%- endfor %}
    default_backend default_backend
backend default_backend
    http-request return status 404 content-type text/plain lf-string "Path \'%[path]\' not found. Ping http://.../-/routes for available routes."
{%- for item in backends_with_health_config %}
{%- set backend = item.backend %}
{%- set hc = item.health_config %}
backend {{ backend.name or 'unknown' }}
    log global
    # Enable HTTP connection reuse for better performance
    http-reuse always
    # Set backend-specific timeouts, overriding defaults if specified
    {%- if backend.timeout_connect_s is not none %}
    timeout connect {{ backend.timeout_connect_s }}s
    {%- endif %}
    {%- if backend.timeout_server_s is not none %}
    timeout server {{ backend.timeout_server_s }}s
    {%- endif %}
    {%- if backend.timeout_client_s is not none %}
    timeout client {{ backend.timeout_client_s }}s
    {%- endif %}
    {%- if backend.timeout_http_request_s is not none %}
    timeout http-request {{ backend.timeout_http_request_s }}s
    {%- endif %}
    {%- if backend.timeout_queue_s is not none %}
    timeout queue {{ backend.timeout_queue_s }}s
    {%- endif %}
    # Set timeouts to support keep-alive connections
    {%- if backend.timeout_http_keep_alive_s is not none %}
    timeout http-keep-alive {{ backend.timeout_http_keep_alive_s }}s
    {%- endif %}
    {%- if backend.timeout_tunnel_s is not none %}
    timeout tunnel {{ backend.timeout_tunnel_s }}s
    {%- endif %}
    # Health check configuration - use backend-specific or global defaults
    {%- if hc.health_path %}
    # HTTP health check with custom path
    option httpchk GET {{ hc.health_path }}
    http-check expect status 200
    {%- endif %}
    {{ hc.default_server_directive }}
    # Servers in this backend
    {%- for server in backend.servers %}
    server {{ server.name }} {{ server.host }}:{{ server.port }} check
    {%- endfor %}
    {%- if backend.fallback_server %}
    # Fallback to head node's Serve proxy when no ingress replicas are available
    server {{ backend.fallback_server.name }} {{ backend.fallback_server.host }}:{{ backend.fallback_server.port }} check backup
    {%- endif %}
{%- if has_ingress_request_router and backend.ingress_request_router_servers %}
backend {{ backend.name or 'unknown' }}-via-ingress-request-router
    log global
    # Keep the pinned data-plane path on the same connection policy as the
    # primary backend. For streamed responses, forcing server-close can leave
    # HAProxy holding unread server-side FINs under a burst while worker
    # threads are still routing other requests.
    http-reuse always
    # use-server falls through to LB if the pinned server is DOWN. Combined
    # with `retry-on` below (when configured), this lets HAProxy redispatch
    # a slow-first-byte request to a different replica instead of head-of-
    # line-blocking on the original pick.
    option redispatch
    {%- if config.ingress_retry_on %}
    retry-on {{ config.ingress_retry_on }}
    {%- endif %}
    {%- if config.ingress_retries is not none %}
    retries {{ config.ingress_retries }}
    {%- endif %}
    {%- if backend.timeout_connect_s is not none %}
    timeout connect {{ backend.timeout_connect_s }}s
    {%- endif %}
    {%- if config.ingress_timeout_server_s is not none %}
    timeout server {{ config.ingress_timeout_server_s }}s
    {%- elif backend.timeout_server_s is not none %}
    timeout server {{ backend.timeout_server_s }}s
    {%- endif %}
    {%- if backend.timeout_http_keep_alive_s is not none %}
    timeout http-keep-alive {{ backend.timeout_http_keep_alive_s }}s
    {%- endif %}
    {%- for server in backend.servers %}
    use-server {{ server.name }} if { var(txn.ingress_request_router_target) -m str "{{ server.name }}" }
    {%- endfor %}
    # `track` allows us to mirror primary-backend health and avoid double-checking.
    {%- for server in backend.servers %}
    server {{ server.name }} {{ server.host }}:{{ server.port }} track {{ backend.name or 'unknown' }}/{{ server.name }}
    {%- endfor %}
    {%- if backend.fallback_server %}
    server {{ backend.fallback_server.name }} {{ backend.fallback_server.host }}:{{ backend.fallback_server.port }} track {{ backend.name or 'unknown' }}/{{ backend.fallback_server.name }} backup
    {%- endif %}
{%- endif %}
{%- endfor %}
{%- if config.grpc_enabled %}
frontend grpc_frontend
    # gRPC requires HTTP/2. HAProxy decodes H2 frames into HTTP request
    # semantics in `mode http` when `proto h2` is on the bind line.
    bind {{ config.grpc_frontend_host }}:{{ config.grpc_frontend_port }} proto h2
    mode http
    log global

{{ grpc_healthz_rules|safe }}

    # ListApplications must aggregate across all apps, so it goes to the
    # head-node fallback Serve proxy rather than an individual replica.
    acl is_list_applications path /ray.serve.RayServeAPIService/ListApplications
{%- if grpc_fallback_server %}
    use_backend grpc_fallback_backend if is_list_applications
{%- else %}
    http-request return status 200 content-type application/grpc hdr grpc-status 14 hdr grpc-message "ListApplications is unavailable" if is_list_applications
{%- endif %}

    # Route per-app on the `application` metadata that Ray Serve clients attach.
{%- for backend in grpc_backends %}
    acl is_{{ backend.name or 'unknown' }} req.hdr(application) -m str {{ backend.app_name }}
    use_backend {{ backend.name or 'unknown' }} if is_{{ backend.name or 'unknown' }}
{%- endfor %}
{%- if grpc_backends|length == 1 %}
    # With exactly one app deployed, route there regardless of metadata so
    # clients can call it without setting the `application` header.
    default_backend {{ grpc_backends[0].name or 'unknown' }}
{%- else %}
    # Zero apps, or multiple apps without a matching `application` header.
    default_backend default_grpc_backend
{%- endif %}

{%- if grpc_fallback_server %}
backend grpc_fallback_backend
    mode http
    log global
    # `proto h2` makes HAProxy speak HTTP/2 cleartext to the fallback gRPC server.
    server {{ grpc_fallback_server.name }} {{ grpc_fallback_server.host }}:{{ grpc_fallback_server.port }} proto h2 check
{%- endif %}
{%- if grpc_backends|length != 1 %}
backend default_grpc_backend
    mode http
    log global
    # Trailers-only NOT_FOUND. gRPC clients surface this as
    # grpc.StatusCode.NOT_FOUND; an HTTP 503 would map to UNAVAILABLE instead.
    acl has_application_header req.hdr(application) -m found
    http-request return status 200 content-type application/grpc hdr grpc-status 5 hdr grpc-message "Application '%[req.hdr(application)]' not found. Ping /ray.serve.RayServeAPIService/ListApplications for available applications." if has_application_header
    http-request return status 200 content-type application/grpc hdr grpc-status 5 hdr grpc-message "Application metadata not set. Ping /ray.serve.RayServeAPIService/ListApplications for available applications."
{%- endif %}
{%- for item in grpc_backends_with_health_config %}
{%- set backend = item.backend %}
{%- set hc = item.health_config %}
backend {{ backend.name or 'unknown' }}
    mode http
    log global
    http-reuse always
    {%- if backend.timeout_connect_s is not none %}
    timeout connect {{ backend.timeout_connect_s }}s
    {%- endif %}
    {%- if backend.timeout_server_s is not none %}
    timeout server {{ backend.timeout_server_s }}s
    {%- endif %}
    {%- if backend.timeout_client_s is not none %}
    timeout client {{ backend.timeout_client_s }}s
    {%- endif %}
    # gRPC health check: POST a Healthz request over HTTP/2 and require the
    # success marker in the response body. HAProxy can't inspect HTTP/2
    # trailers (where gRPC carries grpc-status), so instead rely on the
    # response body to contain the healthy message.
    option httpchk
    http-check connect proto h2
    http-check send meth POST uri /ray.serve.RayServeAPIService/Healthz ver HTTP/2.0 hdr content-type application/grpc hdr te trailers body "\x00\x00\x00\x00\x00"
    http-check expect string {{ healthy_message }}
    {{ hc.default_server_directive }}
    # `proto h2` makes HAProxy speak HTTP/2 cleartext to backend gRPC servers.
    {%- for server in backend.servers %}
    server {{ server.name }} {{ server.host }}:{{ server.port }} proto h2 check
    {%- endfor %}
    {%- if backend.fallback_server %}
    server {{ backend.fallback_server.name }} {{ backend.fallback_server.host }}:{{ backend.fallback_server.port }} proto h2 check backup
    {%- endif %}
{%- endfor %}
{%- endif %}
listen stats
  bind *:{{ config.stats_port }}
  stats enable
  stats uri {{ config.stats_uri }}
  stats refresh 1s
"""
