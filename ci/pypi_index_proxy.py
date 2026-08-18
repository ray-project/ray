#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "asgi-cross-origin-protection>=0.1",
#     "niquests>=3",
#     "starlette>=0.40",
#     "uvicorn>=0.30",
# ]
# ///

# Vendored from https://gist.github.com/thomasdesr/73abd831c1525ad75ac04d11cad3a93a
# Re-sync by overwriting this file from the gist and re-running pre-commit, which
# reorders the imports. One local edit to carry across a re-sync: MIRROR_URL below
# defaults to Ray CI's own mirror rather than the one the gist was written against.
# Runtime deps are pre-installed into /opt/pypiproxy by ci/docker/forge.Dockerfile,
# so this runs under that venv rather than via the `uv run` shebang.
"""Job-local PyPI index proxy.

Serves PyPI simple-index pages fetched through the CI caching mirror, with
every file URL rewritten to point at the mirror, so pip/uv resolve AND
download entirely through the mirror and never contact PyPI. Wheels do not
flow through this process: clients download them straight from the mirror at
the rewritten URLs, and the sha256 hashes in the pages pass through the
rewrite untouched, so artifact verification stays end to end against
upstream bytes.

Usage:
    uv run pypi_index_proxy.py [port]

Listens on 127.0.0.1, on an ephemeral port by default, and prints the bound
address plus ready-to-eval client configuration to stdout:

    listening on http://127.0.0.1:PORT
    export PIP_INDEX_URL=http://127.0.0.1:PORT/simple
    export UV_INDEX_URL=http://127.0.0.1:PORT/simple

Stateless: all caching, deduplication, and stale-on-error availability live
in the mirror. MIRROR_URL is env-overridable only so the proxy can be tested
outside the CI VPC against a stand-in; CI uses the default.
"""

import os
import socket
import sys

import niquests
import uvicorn
from asgi_cross_origin_protection import CrossOriginProtection
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

MIRROR_URL = os.environ.get("MIRROR_URL", "https://mirror.ci.ray.io")

# PyPI serves every artifact from files.pythonhosted.org; the pages carry no
# other artifact host. The replacement is textual on purpose: it covers both
# simple-index representations (HTML and PEP 691 JSON) with one rule and
# survives format additions that real parsing would trip on.
UPSTREAM_FILES_PREFIX = b"https://files.pythonhosted.org/"
MIRROR_FILES_PREFIX = f"{MIRROR_URL}/files.pythonhosted.org/".encode()

# Redirects are followed by default: the mirror serves cache hits as 303s to
# presigned S3 URLs. niquests auto-decompresses, so the rewrite below sees
# plain bytes.
client = niquests.AsyncSession()


async def simple(request: Request) -> Response:
    upstream = f"{MIRROR_URL}/pypi.org/simple/{request.path_params['path']}"
    headers = {}
    # Content negotiation happens on this header: pip/uv choose between the
    # HTML and PEP 691 JSON index representations with it.
    accept = request.headers.get("accept")
    if accept:
        headers["Accept"] = accept
    try:
        response = await client.get(upstream, headers=headers, timeout=60)
    except niquests.exceptions.RequestException as e:
        return PlainTextResponse(f"fetching {upstream} failed: {e}", status_code=502)
    body = response.content
    if response.status_code == 200:
        body = body.replace(UPSTREAM_FILES_PREFIX, MIRROR_FILES_PREFIX)
    return Response(
        body,
        status_code=response.status_code,
        media_type=response.headers.get("content-type", "text/html"),
    )


async def healthz(_request: Request) -> Response:
    return PlainTextResponse("ok")


# Cross-origin protection: rejects cross-site state-changing requests via
# Fetch Metadata. Inert for today's all-GET routes (safe methods are always
# allowed) and starts enforcing if this proxy ever grows one that isn't.
app = CrossOriginProtection(
    Starlette(
        routes=[
            Route("/healthz", healthz),
            Route("/simple/{path:path}", simple),
        ]
    )
)

if __name__ == "__main__":
    # Bind before serving so the ephemeral port is known and announced
    # synchronously: once the lines below are printed, the socket accepts.
    # Bound on all interfaces rather than loopback: the nested containers CI starts
    # for tests have their own loopback, and reaching this on the container's bridge
    # address is what lets them resolve through it without sharing a network
    # namespace. The agent is single-tenant and terminated after one job, and the
    # cross-origin protection below covers the rest.
    listener = socket.create_server(
        ("0.0.0.0", int(sys.argv[1]) if len(sys.argv) > 1 else 0)
    )
    base_url = "http://{}:{}".format(*listener.getsockname())
    print(f"listening on {base_url}", flush=True)
    print(f"export PIP_INDEX_URL={base_url}/simple", flush=True)
    print(f"export UV_INDEX_URL={base_url}/simple", flush=True)
    uvicorn.Server(uvicorn.Config(app, log_level="info")).run(sockets=[listener])
