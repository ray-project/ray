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

HAPROXY_CONFIG_TEMPLATE = """global
    # Log to the standard system log socket with debug level.
    log /dev/log local0 debug
    log 127.0.0.1:{{ config.syslog_port }} local0 debug
    stats socket {{ config.socket_path }} mode 666 level admin expose-fd listeners
    stats timeout 30s
    maxconn {{ config.maxconn }}
    nbthread {{ config.nbthread }}
    {%- if config.enable_hap_optimization %}
    server-state-base {{ config.server_state_base }}
    server-state-file {{ config.server_state_file }}
    {%- endif %}
    {%- if config.hard_stop_after_s is not none %}
    hard-stop-after {{ config.hard_stop_after_s }}s
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
frontend prometheus
    bind :{{ config.metrics_port }}
    mode http
    http-request use-service prometheus-exporter if { path {{ config.metrics_uri }} }
    no log
frontend http_frontend
    bind {{ config.frontend_host }}:{{ config.frontend_port }}
{{ healthz_rules|safe }}
    # Routes endpoint
    acl routes path -i /-/routes
    http-request return status {{ route_info.status }} content-type {{ route_info.routes_content_type }} string "{{ route_info.routes_message }}" if routes

    {%- if config.inject_process_id_header and config.reload_id %}
    # Inject unique reload ID as header to track which HAProxy instance handled the request (testing only)
    http-request set-header x-haproxy-reload-id {{ config.reload_id }}
    {%- endif %}
    # Static routing based on path prefixes in decreasing length then alphabetical order
{%- for backend in backends %}
    acl is_{{ backend.name or 'unknown' }} path_beg {{ '/' if not backend.path_prefix or backend.path_prefix == '/' else backend.path_prefix ~ '/' }}
    acl is_{{ backend.name or 'unknown' }} path {{ backend.path_prefix or '/' }}
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
    balance leastconn
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
    {%- if fallback_server %}
    # Fallback to head node's Serve proxy when no ingress replicas are available
    server {{ fallback_server.name }} {{ fallback_server.host }}:{{ fallback_server.port }} check backup
    {%- endif %}
{%- endfor %}
listen stats
  bind *:{{ config.stats_port }}
  stats enable
  stats uri {{ config.stats_uri }}
  stats refresh 1s
"""
