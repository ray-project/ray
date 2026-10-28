---
myst:
  html_meta:
    description: "How Ray token authentication works: authentication modes, token sources and precedence, and propagation across C++, Python, and HTTP."
---

(token-authentication)=

# Token authentication

As of Ray 2.52.0, Ray supports token authentication. With token authentication enabled, Ray requires a single, statically generated token in the authorization header of every request to the Ray dashboard, the GCS server, and other control-plane services.

This page describes the design and architecture of token authentication in Ray, including configuration, token loading, propagation, and verification across C++, Python, and the Ray dashboard.

## Authentication modes

The `RAY_AUTH_MODE` environment variable controls Ray's authentication behavior. Ray supports two modes:

- `token`: Static bearer token authentication. The default for local clusters starting in Ray 2.59, and for all clusters starting in Ray 2.61.
- `disabled`: No authentication. The default for remote and multi-node clusters until Ray 2.61, and the explicit opt-out after that.

Set `RAY_AUTH_MODE` through the environment, and set it consistently on every node in the Ray cluster. When you set `RAY_AUTH_MODE=token`, Ray turns on token authentication, and all supported RPC and HTTP entry points enforce token-based authentication.

## Token sources and precedence

When token authentication is enabled, Ray looks for the token in the following order, from highest to lowest precedence:

1. `RAY_AUTH_TOKEN` environment variable: If this variable is set and non-empty, Ray uses its value directly as the token string.

1. `RAY_AUTH_TOKEN_PATH` environment variable: This variable points to a file. If it's set, Ray reads the token from that file. If Ray can't read the file or the file is empty, Ray treats this as a fatal misconfiguration and aborts rather than silently falling back.

1. Default token file: If neither of the preceding variables is set, Ray falls back to a default path:

   - `~/.ray/auth_token` on POSIX systems
   - `%USERPROFILE%\.ray\auth_token` on Windows

When you start a local cluster with `ray.init()` and authentication enabled, Ray automatically generates a token and persists it at the default path if no token exists.

:::{note}
Ray strips whitespace when it reads the token from a file, which avoids issues from trailing newlines.
:::

## Token propagation and verification

The following sections describe the token format that servers expect, and how C++, Python, and HTTP clients and servers attach and verify the token.

### Common expectations

In both C++ and Python, gRPC servers expect the token in the authorization metadata key, in the following form:

```text
Authorization: Bearer <token_value>
```

HTTP servers expect one of the following forms:

- `Authorization: Bearer <token>`: The Ray CLI and other internal HTTP clients use this form.
- Cookie `ray-authentication-token=<token>`: The browser-based dashboard uses this form.
- `X-Ray-Authorization: Bearer <token>`: KubeRay uses this form, as do environments where a proxy might strip the standard `Authorization` header.

### C++ clients and servers

On the C++ side, Ray uses gRPC's interceptor API to attach the token to outgoing RPCs automatically. Ray defines the client interceptor in [token_auth_client_interceptor.h](https://github.com/ray-project/ray/blob/master/src/ray/rpc/authentication/token_auth_client_interceptor.h).

Create all production C++ gRPC channels through the `BuildChannel()` helper, which wires in the interceptor when token authentication is enabled. Don't create channels directly with `grpc::CreateCustomChannel`, because that bypasses token attachment. `BuildChannel()` is the central enforcement point that ensures all C++ clients automatically add the correct `Authorization: Bearer <token>` metadata.

Server-side token validation compares the token that the client presents with the token that the cluster started with. This check runs in [server_call.h](https://github.com/ray-project/ray/blob/master/src/ray/rpc/server_call.h), inside the generic request-handling path. Because all gRPC services inherit from the same base call implementation, the validation applies uniformly to all C++ gRPC servers when token authentication is enabled.

### Python clients and servers

Most Python components use Cython bindings over the C++ clients, so they automatically inherit the same token behavior without additional Python-level code.

For components that construct gRPC clients or servers directly in Python, explicit synchronous and asynchronous interceptors add and validate authentication metadata. The interceptors live in the following modules:

- [Client interceptors](https://github.com/ray-project/ray/blob/master/python/ray/_private/authentication/grpc_authentication_client_interceptor.py)
- [Server interceptors](https://github.com/ray-project/ray/blob/master/python/ray/_private/authentication/grpc_authentication_server_interceptor.py)

Create all Python gRPC clients and servers with the helper utilities in [grpc_utils.py](https://github.com/ray-project/ray/blob/master/python/ray/_private/grpc_utils.py). These helpers automatically attach the correct client or server interceptors when token authentication is enabled. Always go through the shared utilities so that Ray enforces authentication consistently, and never construct raw gRPC channels or servers directly.

### HTTP clients and servers

For HTTP services, aiohttp middleware in [http_token_authentication.py](https://github.com/ray-project/ray/blob/master/python/ray/_private/authentication/http_token_authentication.py) implements token authentication.

You must add the middleware explicitly to each server's middleware list, as in the `dashboard_head` and `runtime_env_agent` services. After you add it, the middleware does the following:

- Extracts the token from the `Authorization` header, the `X-Ray-Authorization` header, or the `ray-authentication-token` cookie.
- Validates the token and returns the following status codes:

  - `401 Unauthorized` for a missing token
  - `403 Forbidden` for an invalid token

On the client side, HTTP callers can attach headers with the `get_auth_headers_if_auth_enabled()` helper. If token authentication is enabled, this helper computes `Authorization: Bearer <token>` and merges it with any headers the caller supplies.

## Ray dashboard flow

When you start a Ray cluster with `RAY_AUTH_MODE=token`, opening the dashboard triggers the following authentication flow in the UI:

1. The dashboard shows a dialog that prompts you to enter the authentication token.
1. After you submit the token, the frontend sends a `POST` request with the `Authorization: Bearer <token>` header to the dashboard head's `/api/authenticate` endpoint.
1. The dashboard head validates the token.
1. If validation succeeds, the server responds with `200 OK` and instructs the browser to set a cookie with the following properties:

   - Name: `ray-authentication-token`
   - Value: `<token>`
   - Attributes: `HttpOnly` and `SameSite=Strict`, plus `Secure` over HTTPS
   - `max_age`: 30 days

From then on, dashboard UI API calls automatically include the cookie and pass the middleware's authentication checks.

If a backend request returns `401 Unauthorized` for a missing token, or `403 Forbidden` for an invalid token or a mode change, the dashboard UI treats it as an authentication failure. The UI clears any stale state and reopens the authentication dialog, which prompts you to enter a valid token again.

This approach keeps the token out of JavaScript-accessible storage and relies on standard browser cookie mechanics to secure subsequent requests.

## Ray CLI

Ray CLI commands that talk to an authenticated cluster automatically load the token from `RAY_AUTH_TOKEN`, `RAY_AUTH_TOKEN_PATH`, or the default token file. They check these three sources in the precedence order described earlier on this page.

After loading the token, CLI commands pass it to their internal RPC calls. Depending on the underlying implementation, they do one of the following:

- Use C++ clients, and therefore the C++ interceptors through `BuildChannel()`.
- Use Python gRPC clients or servers, and the Python interceptors through `grpc_utils.py`.
- Use HTTP helpers that call `get_auth_headers_if_auth_enabled()`.

As long as you configure the token through one of the supported sources, the CLI works against token-secured clusters.

### ray get-auth-token command

Run the `ray get-auth-token` command to retrieve and share the token that a local Ray cluster uses. For example, you might paste the token into the dashboard UI.

By default, `ray get-auth-token` attempts to load an existing token from `RAY_AUTH_TOKEN`, `RAY_AUTH_TOKEN_PATH`, or the default token file.

If the command finds a token, it prints the token to `stdout` in a form suitable for scripting and export. If no token exists, the command fails with an error explaining that no token is configured.

Pass the `--generate` flag to generate a token and store it in the default token file if no token is configured. The flag doesn't overwrite an existing token. It only creates one when none is present.

## Adding token authentication to new services

When you add a gRPC or HTTP service to Ray, follow the guidelines in this section so that the service supports token authentication.

### gRPC services

For C++ services, follow these guidelines:

- Always create gRPC channels through `BuildChannel()`. Never use `grpc::CreateCustomChannel` directly.
- Server-side validation is automatic if your service inherits from the standard base call implementation.

For Python services, follow these guidelines:

- Use helper utilities from `grpc_utils.py` to create clients and servers.
- The helpers attach the interceptors automatically when token authentication is enabled.

### HTTP services

For HTTP services, do the following:

- Add the authentication middleware from `http_token_authentication.py` to your server's middleware list.
- Use `get_auth_headers_if_auth_enabled()` for client-side header attachment.

:::{note}
Ray doesn't wire up HTTP middleware and header injection automatically. Add them manually to each new HTTP service.
:::
