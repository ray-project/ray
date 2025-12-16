.. _token-authentication:

Token Authentication
====================

Ray v2.52.0 introduced support for token authentication, enabling Ray to enforce the use of a
single, statically generated token in the authorization header for all requests to the Ray
Dashboard, GCS server, and other control-plane services.

This document covers the design and architecture of token authentication in Ray, including
configuration, token loading, propagation, and verification across C++, Python, and the Ray dashboard.

Authentication Modes
--------------------

Ray's authentication behavior is controlled by the **RAY_AUTH_MODE** environment variable.
As of now, Ray supports two modes:

- ``disabled`` - Default; no authentication.
- ``token`` - Static bearer token authentication.

**RAY_AUTH_MODE** must be set via the environment and should be configured consistently on every
node in the Ray cluster. When ``RAY_AUTH_MODE=token``, token authentication is enabled and all
supported RPC and HTTP entry points enforce token based authentication.

Token Sources and Precedence
----------------------------

Once token auth is enabled, Ray looks for the token in the following order (highest to lowest
precedence):

1. **RAY_AUTH_TOKEN** (environment variable): If set and non-empty, this value is used directly
   as the token string.

2. **RAY_AUTH_TOKEN_PATH** (environment variable pointing to file): If set, Ray reads the token
   from that file. If the file cannot be read or is empty, Ray treats this as a fatal
   misconfiguration and aborts rather than silently falling back.

3. **Default token file path**: If neither of the above are set, Ray falls back to a default path:

   - ``~/.ray/auth_token`` on POSIX systems
   - ``%USERPROFILE%\.ray\auth_token`` on Windows

For local clusters started with ``ray.init()`` and auth enabled, Ray automatically generates a
new token and persists it at the default path if no token exists.

.. note::
   Whitespace is stripped when reading the token from files to avoid issues from trailing newlines.

Token Propagation and Verification
----------------------------------

Common Expectations
~~~~~~~~~~~~~~~~~~~

Across both C++ and Python, gRPC servers expect the token to be present in the authorization
metadata key as:

.. code-block:: text

   Authorization: Bearer <token_value>

HTTP servers similarly expect one of:

1. ``Authorization: Bearer <token>`` - Used by Ray CLI and other internal HTTP clients.
2. Cookie ``ray-authentication-token=<token>`` - Used by the browser-based dashboard.
3. ``X-Ray-Authorization: Bearer <token>`` - Used by KubeRay and environments where the standard
   ``Authorization`` header may be stripped by a proxy.

C++ Clients and Servers
~~~~~~~~~~~~~~~~~~~~~~~

On the C++ side, token attachment to outgoing RPCs is automated using gRPC's interceptor API.
The client interceptor is defined in
`token_auth_client_interceptor.h <https://github.com/ray-project/ray/blob/master/src/ray/rpc/authentication/token_auth_client_interceptor.h>`_.

All production C++ gRPC channels must be created through the ``BuildChannel()`` helper, which
wires in the interceptor when token auth is enabled. Ray developers must not create channels
directly with ``grpc::CreateCustomChannel``; doing so would bypass token attachment.
``BuildChannel()`` is the central enforcement point that ensures all C++ clients automatically
add the correct ``Authorization: Bearer <token>`` metadata.

Server-side token validation compares the token presented by the client with the token the
cluster was started with. This check is performed in
`server_call.h <https://github.com/ray-project/ray/blob/master/src/ray/rpc/server_call.h>`_
inside the generic request handling path. Because all gRPC services inherit from the same base
call implementation, the validation applies uniformly to all C++ gRPC servers when token auth
is enabled.

Python Clients and Servers
~~~~~~~~~~~~~~~~~~~~~~~~~~

Most Python components use Cython bindings over the C++ clients, so they automatically inherit
the same token behavior without additional Python-level code.

For components that construct gRPC clients or servers directly in Python, explicit interceptors
(both sync and async) add and validate authentication metadata:

- `Client interceptors <https://github.com/ray-project/ray/blob/master/python/ray/_private/authentication/grpc_authentication_client_interceptor.py>`_
- `Server interceptors <https://github.com/ray-project/ray/blob/master/python/ray/_private/authentication/grpc_authentication_server_interceptor.py>`_

All Python gRPC clients and servers should be created using helper utilities from
`grpc_utils.py <https://github.com/ray-project/ray/blob/master/python/ray/_private/grpc_utils.py>`_.
These helpers automatically attach the correct client/server interceptors when token auth is
enabled. The convention is to always go through the shared utilities so that auth is consistently
enforced, never constructing raw gRPC channels or servers directly.

HTTP Clients and Servers
~~~~~~~~~~~~~~~~~~~~~~~~

For HTTP services, token authentication is implemented using aiohttp middleware in
`http_token_authentication.py <https://github.com/ray-project/ray/blob/master/python/ray/_private/authentication/http_token_authentication.py>`_.

The middleware must be explicitly added to each server's middleware list (e.g., ``dashboard_head``
service and ``runtime_env_agent`` service). Once configured, it:

- Extracts the token from ``Authorization`` header, ``X-Ray-Authorization`` header, or
  ``ray-authentication-token`` cookie.
- Validates the token and returns:

  - **401 Unauthorized** for missing token.
  - **403 Forbidden** for invalid token.

Client-side, HTTP callers can use the ``get_auth_headers_if_auth_enabled()`` helper to attach
headers. This helper computes ``Authorization: Bearer <token>`` if token auth is enabled and
merges it with any user-supplied headers.

.. note::
   For HTTP, middleware and header injection are not automatically wired up for new services;
   they must be added manually.

Ray Dashboard Flow
------------------

When a Ray cluster is started with ``RAY_AUTH_MODE=token``, accessing the dashboard triggers an
authentication flow in the UI:

1. The user sees a dialog prompting them to enter the authentication token.
2. Once the user submits the token, the frontend sends a ``POST`` request to the dashboard head's
   ``/api/authenticate`` endpoint with ``Authorization: Bearer <token>`` header.
3. The dashboard head validates the token.
4. If validation succeeds, the server responds with **200 OK** and instructs the browser to set
   a cookie:

   - Name: ``ray-authentication-token``
   - Value: ``<token>``
   - Attributes: ``HttpOnly``, ``SameSite=Strict`` (and ``Secure`` when running over HTTPS)
   - max_age: 30 days (cookie is cleared after 30 days)

From this point on, subsequent dashboard UI API calls automatically include the cookie and
satisfy the middleware's authentication checks.

If a backend request returns **401 Unauthorized** (no token) or **403 Forbidden** (invalid token
or mode change), the dashboard UI interprets this as an authentication failure. It clears any
stale state and re-opens the authentication dialog, prompting the user to re-enter a valid token.

This approach keeps the token out of JavaScript-accessible storage and relies on standard browser
cookie mechanics to secure subsequent requests.

Ray CLI
-------

Ray CLI commands that talk to an authenticated cluster automatically load the token from the same
three mechanisms (in the same precedence order):

- **RAY_AUTH_TOKEN**, **RAY_AUTH_TOKEN_PATH**, or the default token file.

Once loaded, CLI commands pass the token along to their internal RPC calls. Depending on the
underlying implementation, they either:

- Use C++ clients (and thus C++ interceptors via ``BuildChannel()``), or
- Use Python gRPC clients/servers and the Python interceptors via ``grpc_utils.py``, or
- Use HTTP helpers that call ``get_auth_headers_if_auth_enabled()``.

From the user's perspective, as long as the token is configured via one of the supported
mechanisms, the CLI works against token-secured clusters.

ray get-auth-token Command
~~~~~~~~~~~~~~~~~~~~~~~~~~

To retrieve and share the token used by a local Ray cluster (for example, to paste into the
dashboard UI), Ray provides the ``ray get-auth-token`` command.

By default, ``ray get-auth-token`` attempts to load an existing token from:

- **RAY_AUTH_TOKEN**, **RAY_AUTH_TOKEN_PATH**, or the default token file.

If a token is found, it is printed to ``stdout`` (suitable for scripting and export). If no
token exists, the command fails with an error explaining that no token is configured.

Users can pass the ``--generate`` flag to generate a new token and store it in the default
token file path if no token is currently configured. This does not overwrite an existing token;
it only creates one when none is present.

Adding Token Authentication to New Services
-------------------------------------------

When adding new gRPC or HTTP services to Ray, follow these guidelines to ensure proper token
authentication support:

gRPC Services
~~~~~~~~~~~~~

**C++ Services:**

1. Always create gRPC channels through ``BuildChannel()`` - never use ``grpc::CreateCustomChannel``
   directly.
2. Server-side validation is automatic if your service inherits from the standard base call
   implementation.

**Python Services:**

1. Use helper utilities from ``grpc_utils.py`` to create clients and servers.
2. The interceptors are automatically attached when token auth is enabled.

HTTP Services
~~~~~~~~~~~~~

1. Add the authentication middleware from ``http_token_authentication.py`` to your server's
   middleware list.
2. Use ``get_auth_headers_if_auth_enabled()`` for client-side header attachment.

.. note::
   HTTP middleware and header injection are not automatically wired up - they must be added
   manually to each new HTTP service.
