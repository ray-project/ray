"""
Web search + URL fetch MCP server deployed with Ray Serve.

Tools exposed:
  - brave_search: Uses Brave Search API
  - fetch_url: Fetches a URL and returns simplified markdown (or raw) content, with optional robots.txt checks

Environment variables:
  - BRAVE_API_KEY: Brave Search API subscription token
  - WEB_SEARCH_USER_AGENT: User-Agent string used for outbound HTTP requests
  - WEB_SEARCH_PROXY_URL: Optional proxy URL (passed to httpx AsyncClient)
  - WEB_SEARCH_IGNORE_ROBOTS_TXT: "true"/"false" (default: false)
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import quote_plus, urlparse, urlunparse

import httpx
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from protego import Protego
from ray import serve

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------

BRAVE_API_KEY = os.getenv("BRAVE_API_KEY", "")

DEFAULT_USER_AGENT = "ModelContextProtocol/1.0 (WebSearch; +https://github.com/modelcontextprotocol/servers)"
USER_AGENT = os.getenv("WEB_SEARCH_USER_AGENT", DEFAULT_USER_AGENT)

PROXY_URL = os.getenv("WEB_SEARCH_PROXY_URL") or None

IGNORE_ROBOTS_TXT = (os.getenv("WEB_SEARCH_IGNORE_ROBOTS_TXT", "false").strip().lower() in {"1", "true", "yes"})

BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _get_robots_txt_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, "/robots.txt", "", "", ""))


async def _check_may_fetch_url(url: str, user_agent: str) -> None:
    """Raise ValueError if robots.txt forbids fetching this URL for the given user agent."""
    robots_url = _get_robots_txt_url(url)
    async with httpx.AsyncClient(proxy=PROXY_URL, timeout=30.0, follow_redirects=True) as client:
        try:
            resp = await client.get(robots_url, headers={"User-Agent": user_agent})
        except Exception as e:
            # If robots.txt can't be fetched due to connection issues, be conservative and block.
            raise ValueError(f"Failed to fetch robots.txt ({robots_url}): {e!r}")

        # If robots.txt is missing (404) or other 4xx besides auth, allow.
        if 400 <= resp.status_code < 500 and resp.status_code not in (401, 403):
            return
        if resp.status_code in (401, 403):
            raise ValueError(
                f"Robots check blocked: fetching robots.txt ({robots_url}) returned HTTP {resp.status_code}."
            )
        if resp.status_code >= 500:
            raise ValueError(f"Robots check blocked: fetching robots.txt ({robots_url}) returned HTTP {resp.status_code}.")

        robots_txt = resp.text

    processed = "\n".join(line for line in robots_txt.splitlines() if not line.strip().startswith("#"))
    parser = Protego.parse(processed)
    if not parser.can_fetch(url, user_agent):
        raise ValueError(
            "Robots check blocked: site robots.txt disallows fetching this URL for the current user agent. "
            f"robots.txt={robots_url} url={url}"
        )


def _extract_markdown_from_html(html: str) -> str:
    """
    Convert HTML to simplified markdown using Readability.
    Falls back to a short error string if parsing fails.
    """
    try:
        import markdownify
        import readabilipy.simple_json

        ret = readabilipy.simple_json.simple_json_from_html_string(html, use_readability=True)
        if not ret.get("content"):
            return "<error>Page failed to be simplified from HTML</error>"
        return markdownify.markdownify(ret["content"], heading_style=markdownify.ATX)
    except Exception:
        return "<error>Failed to simplify HTML to markdown</error>"


def _truncate(content: str, *, max_length: int, start_index: int) -> tuple[str, str]:
    """
    Returns (chunk, suffix_note).
    """
    original_length = len(content)
    if start_index >= original_length:
        return "<error>No more content available.</error>", ""

    chunk = content[start_index : start_index + max_length]
    if not chunk:
        return "<error>No more content available.</error>", ""

    remaining = original_length - (start_index + len(chunk))
    if len(chunk) == max_length and remaining > 0:
        next_start = start_index + len(chunk)
        return (
            chunk,
            f"\n\n<error>Content truncated. Call fetch_url with start_index={next_start} to get more.</error>",
        )
    return chunk, ""


async def _http_get_text(url: str, *, user_agent: str, timeout_s: float = 30.0, headers: dict | None = None) -> tuple[str, str]:
    """
    Fetch URL. Returns (text, content_type).
    """
    request_headers = {"User-Agent": user_agent}
    if headers:
        request_headers.update(headers)
    async with httpx.AsyncClient(proxy=PROXY_URL, timeout=timeout_s, follow_redirects=True) as client:
        resp = await client.get(url, headers=request_headers)
        resp.raise_for_status()
        return resp.text, (resp.headers.get("content-type") or "")


# ----------------------------------------------------------------------
# MCP server
# ----------------------------------------------------------------------

mcp = FastMCP("web_search", stateless_http=True)


@mcp.tool()
async def brave_search(query: str, num_results: int = 10) -> str:
    """
    Search the web through the Brave Search API.

    Requires env var:
      - BRAVE_API_KEY

    Args:
      query: Search query string.
      num_results: Total number of results to return (1-20).

    Returns:
      JSON string: [{"title": "...", "link": "...", "snippet": "..."}, ...]
    """
    if not query.strip():
        return "Query must be non-empty."

    num_results = int(num_results)
    if num_results < 1:
        return "num_results must be >= 1."
    if num_results > 20:
        num_results = 20  # Brave API max per request

    brave_headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }
    params = f"?q={quote_plus(query)}&count={num_results}"
    url = f"{BRAVE_SEARCH_URL}{params}"

    try:
        text, _ = await _http_get_text(url, user_agent=USER_AGENT, headers=brave_headers)
        data = json.loads(text)
    except httpx.HTTPStatusError as e:
        return f"Brave search request failed: HTTP {e.response.status_code}"
    except Exception:
        return "Brave search request failed. Check server logs for details."

    web_results = (data.get("web") or {}).get("results") or []
    results: list[dict[str, Any]] = []
    for item in web_results:
        results.append(
            {
                "title": item.get("title", ""),
                "link": item.get("url", ""),
                "snippet": item.get("description", ""),
            }
        )

    return json.dumps(results, ensure_ascii=False)


@mcp.tool()
async def fetch_url(
    url: str,
    max_length: int = 5000,
    start_index: int = 0,
    raw: bool = False,
    ignore_robots_txt: bool = False,
) -> str:
    """
    Fetch a URL and return its content (optionally simplified to markdown).

    Args:
      url: URL to fetch.
      max_length: Maximum number of characters to return (1..1_000_000).
      start_index: Start returning content from this character offset (>=0).
      raw: If true, return raw page content (HTML/text) without simplification.
      ignore_robots_txt: If true, skip robots.txt checks for this request.
    """
    url = (url or "").strip()
    if not url:
        return "url is required."

    if max_length < 1 or max_length > 1_000_000:
        return "max_length must be between 1 and 1_000_000."
    if start_index < 0:
        return "start_index must be >= 0."

    # robots.txt gate
    if not (IGNORE_ROBOTS_TXT or ignore_robots_txt):
        try:
            await _check_may_fetch_url(url, USER_AGENT)
        except Exception as e:
            return f"Fetch blocked by robots.txt: {e}"

    try:
        page_raw, content_type = await _http_get_text(url, user_agent=USER_AGENT)
    except httpx.HTTPStatusError as e:
        return f"Failed to fetch {url} - status code {e.response.status_code}"
    except Exception as e:
        return f"Failed to fetch {url}: {e!r}"

    is_html = ("<html" in page_raw[:200].lower()) or ("text/html" in content_type.lower()) or (not content_type)
    if is_html and not raw:
        content = _extract_markdown_from_html(page_raw)
        prefix = ""
    else:
        content = page_raw
        prefix = (
            f"Content type {content_type} cannot be simplified to markdown, but here is the raw content:\n"
            if is_html and raw is False
            else ""
        )

    chunk, suffix = _truncate(content, max_length=max_length, start_index=start_index)
    return f"{prefix}Contents of {url}:\n{chunk}{suffix}"


# ----------------------------------------------------------------------
# FastAPI + Ray Serve
# ----------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.mount("/", mcp.streamable_http_app())
    async with mcp.session_manager.run():
        yield


fastapi_app = FastAPI(lifespan=lifespan)


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 20,
        "target_ongoing_requests": 5,
    },
    ray_actor_options={"num_cpus": 0.2},
)
@serve.ingress(fastapi_app)
class WebSearchMCP:
    def __init__(self):
        pass


app = WebSearchMCP.bind()
