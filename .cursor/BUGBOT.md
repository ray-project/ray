# Bugbot Rules

## Rule: Redact runtime envs before they reach unauthenticated sinks
- `runtime_env={"env_vars": {...}}` is the documented way users pass credentials to jobs, actors and tasks, so a `runtime_env` (as a dict, a serialized JSON string, a `RuntimeEnvInfo`, or a `RuntimeEnvContext`) routinely carries cloud keys, DB passwords and API tokens.
- Look at C++ and Python changes that write a `runtime_env` or its `env_vars` to any sink that isn't protected by Ray's auth token. These sinks include:
  - Log lines (`RAY_LOG`, `RAY_CHECK`/`RAY_CHECK_*` messages, `logger.*`, `print`), because the dashboard serves log files.
  - Exception, `Status` and error messages, because they end up in logs and in task and actor errors.
  - Process command lines and subprocess arguments, because the reporter agent shows them in the dashboard.
  - Events or payloads exported to external endpoints.
  - Dashboard HTTP responses to browser-originated requests (gate on `should_redact_runtime_env(req)`).
  - Debug strings such as `DebugString()`, `__repr__` and `__str__` that feed any of the above.
- If the value isn't passed through the redaction helpers first, post the following message:

> ⚠️ This change writes a `runtime_env` to a sink that isn't protected by Ray's auth token. Its `env_vars` values may hold user credentials. Redact it first:
> Redact only the copy you print. Leave the original untouched, because Ray launches workers from it.

- Don't post this message for runtime envs sent between Ray components over authenticated gRPC, stored in GCS, or used to launch workers. Those paths must keep the plaintext values.

## Rule: Keep C++ and Python runtime env redaction consistent
- Look at the list of changed files in the PR.
- If either `src/ray/common/runtime_env_redaction.{h,cc}` or `python/ray/_private/runtime_env/redaction.py` is changed and the other isn't, compare their behavior. They must agree on:
  - The placeholder (`kRedactedPlaceholder` / `REDACTED_PLACEHOLDER`).
  - The set of secret fields (`kSecretRuntimeEnvFields` / `SECRET_RUNTIME_ENV_FIELDS`).
  - What is masked: values are replaced, keys are kept.
  - Fail-closed handling: input that is empty, isn't valid JSON, or isn't a JSON object.
  - When the input is returned unchanged instead of re-serialized.
- If the change makes the two diverge, post the following message:

> ⚠️ This PR changes runtime env redaction in only one language. `src/ray/common/runtime_env_redaction.{h,cc}` and `python/ray/_private/runtime_env/redaction.py` must mask the same fields with the same placeholder and fail closed the same way. Otherwise a secret redacted on one side leaks from the other. Please make the matching change on the other side and update both `src/ray/common/tests/runtime_env_redaction_test.cc` and the Python redaction tests.

- If both files are changed, check that they still agree on the points above, and post the same message if they don't.

## Rule: New HTTP endpoints require the auth token
- Look at Python changes that start a new HTTP server or add routes to one, including Prometheus metrics endpoints.
- If a new aiohttp app isn't built with `get_token_auth_middleware`, a new metrics endpoint isn't served through `start_metrics_http_server`, `start_metrics_wsgi_server` or `get_token_auth_wsgi_app` (all in `python/ray/_private/authentication/http_token_authentication.py`), or a path is added to a server's public (unauthenticated) path list, post the following message:

> ⚠️ This change adds an HTTP endpoint that may answer without Ray's auth token. With token auth on, every Ray HTTP endpoint, including metrics, must require the token. Serve aiohttp apps behind `get_token_auth_middleware`, and metrics through `start_metrics_http_server`, `start_metrics_wsgi_server` or `get_token_auth_wsgi_app`. Make a path public only if, like the existing public paths, it serves static assets, a health check, the auth mode or the login endpoint, and open metrics only with `RAY_AUTH_SKIP_METRICS=1`.

- Don't post this message for the routes of user applications that Ray Serve serves. Serve's metrics endpoints are still covered.

## Rule: Keep the auth token off command lines and out of logs
- Look at C++ and Python changes that read the auth token (`RAY_AUTH_TOKEN`, a token file, `AuthenticationTokenLoader` and its Python binding's `get_raw_token()`) or pass it to another process.
- If the token's value reaches a command line or subprocess argument, a log line, an exception or `Status` message, or a new file Ray writes, post the following message:

> ⚠️ This change exposes Ray's auth token. Command lines show up in `ps` and in the dashboard, and log lines and error messages end up in log files the dashboard serves. Hand a child process the path of an existing token file (`RAY_AUTH_TOKEN_PATH` or a file mount), or the name of the `RAY_AUTH_TOKEN` variable for it to read from its environment, and don't write a new copy of the token. The one exception is the mode-0600 `ray_prometheus.yml` that `ray metrics launch-prometheus` writes when `RAY_AUTH_TOKEN` is set.

## Rule: Connecting code never creates an auth token
- Look at C++ and Python changes to code that connects to an existing cluster: worker nodes, drivers that connect with an address, `ray://` clients, CLI commands and SDKs.
- If that code calls `CreateTokenIfMissing()` or Python's `create_token_if_missing()`, runs `ray get-auth-token --generate`, or otherwise generates or writes a token, post the following message:

> ⚠️ This change creates an auth token on a path that connects to an existing cluster. A token created there can't match the cluster's, so the connection fails later with a confusing mismatch. Fail with the shared auth error instead. Only the existing creators make a token, all through `AuthenticationTokenLoader::CreateTokenIfMissing()`: a bare `ray.init()` that starts a new cluster, `ray.cluster_utils.Cluster()` and `AutoscalingCluster`, C++ and Java drivers that start their own cluster, `ray get-auth-token --generate`, and `ray up`. `ray start --head` never creates one.

## Rule: Auth rejections are final
- Look at C++ and Python changes to connect, reconnect and retry logic, and to code that catches `ConnectionError`.
- If the change retries a request or connection after an auth rejection (`AuthenticationError` in Python; `Status::IsUnauthenticated()` or gRPC `UNAUTHENTICATED` in C++), or catches `ConnectionError` and wraps, retries or swallows it without re-raising `AuthenticationError` unchanged, post the following message:

> ⚠️ This change can retry or hide an auth rejection. A rejected node, driver or client must fail at once with the auth error, which names the fix, instead of hanging or failing later with a generic message. `AuthenticationError` subclasses `ConnectionError`, so a `ConnectionError` handler that wraps or retries must re-raise `AuthenticationError` unchanged. In C++, treat `Unauthenticated` as terminal, not retryable.
