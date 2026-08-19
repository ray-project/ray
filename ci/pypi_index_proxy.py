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
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

# Optional because it is the one dependency here that requires Python >= 3.11, and the
# surfaces that need this proxy most are the ones without a modern interpreter: the
# macOS agents carry 3.9 and 3.10 only, and the Windows agent host 3.8. Everything else
# it needs supports 3.10. The middleware rejects cross-site state-changing requests via
# Fetch Metadata, and every route here is a GET, which it always allows -- so where it
# is absent nothing it would have done is skipped. Images that do have 3.11+ still
# install it, and then it behaves exactly as before.
try:
    from asgi_cross_origin_protection import CrossOriginProtection
except ImportError:  # pragma: no cover - depends on the interpreter, not the code path

    def CrossOriginProtection(app):
        return app


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
    # Always ask upstream for HTML, ignoring what the client negotiated. The mirror
    # caches /simple/ keyed on the URL alone with no Vary, so whichever
    # representation is fetched first is what every later client gets. Forwarding the
    # client's Accept therefore poisons the entry: uv asks for PEP 691 JSON, the
    # mirror stores JSON, and then whl_library's pip -- which supports only
    # text/html -- skips the page and reports "from versions: none". Seen on
    # postmerge 19272:
    #
    #   WARNING: Skipping page .../simple/exceptiongroup/ because the GET request got
    #   Content-Type: application/vnd.pypi.simple.v1+json. The only supported
    #   Content-Type is text/html
    #
    # HTML rather than JSON because it is the representation every client here
    # understands: that pip cannot read JSON at all, while uv reads both.
    headers = {"Accept": "text/html"}
    try:
        response = await client.get(upstream, headers=headers, timeout=60)
    except niquests.exceptions.RequestException as e:
        return PlainTextResponse(f"fetching {upstream} failed: {e}", status_code=502)
    body = response.content
    if response.status_code == 200:
        body = body.replace(UPSTREAM_FILES_PREFIX, MIRROR_FILES_PREFIX)
    # `or` rather than a default: covers a header that is present but empty, which a
    # default does not.
    content_type = response.headers.get("content-type") or "text/html"
    # Asking for HTML and getting something else means the mirror is holding a
    # representation cached before this pinning landed. Pinning stops new entries going
    # in but cannot evict old ones, since the Accept header is not part of the mirror's
    # cache key -- those have to be deleted from the cache prefix. Say so, because the
    # symptom at the client is the misleading "from versions: none".
    # Lowercased because header values are case-insensitive, and a false positive here
    # tells the reader to delete cache entries that are in fact fine.
    if response.status_code == 200 and "html" not in content_type.lower():
        print(
            f"pypi proxy: {upstream} returned {content_type} for an HTML request; "
            "this is a stale mirror cache entry and clients that only read HTML will "
            "skip it. Delete the cache/pypi.org/simple/ prefix to clear it.",
            flush=True,
        )
    return Response(body, status_code=response.status_code, media_type=content_type)


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
