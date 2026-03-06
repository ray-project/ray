"""
Research agent that uses a Web Search MCP server (brave_search + fetch_url)
to perform online research and gather sources.
"""

from __future__ import annotations

import os

from agent_runtime.agent_builder import build_mcp_agent
from agent_runtime.config import MCPEndpoint


# ========== MCP CONFIG ==========
# Web Search MCP server endpoint configuration
WEB_SEARCH_MCP_BASE_URL = os.getenv("WEB_SEARCH_MCP_BASE_URL", "").strip().rstrip("/")
WEB_SEARCH_MCP_TOKEN = os.getenv("WEB_SEARCH_MCP_TOKEN", "").strip()


def _web_search_mcp_endpoint() -> MCPEndpoint:
    """Build Web Search MCP endpoint configuration."""
    return MCPEndpoint(
        name="web_search",
        base_url=WEB_SEARCH_MCP_BASE_URL,
        token=WEB_SEARCH_MCP_TOKEN,
    )


# ========== SYSTEM PROMPT ==========
PROMPT = (
    "You are a careful research assistant.\n"
    "\n"
    "You can use MCP tools for online research:\n"
    "- brave_search(query, num_results)\n"
    "- fetch_url(url, max_length, start_index, raw, ignore_robots_txt)\n"
    "\n"
    "Rules:\n"
    "- Break the task into sub-questions.\n"
    "- Use brave_search first to find relevant sources.\n"
    "- Use fetch_url to read primary sources and confirm details.\n"
    "- Don't fabricate. If you can't verify something, say so.\n"
    "- In the final answer, include sources as a short bullet list of URLs.\n"
    "- Only output the final answer (no hidden thoughts).\n"
)


# ========== BUILD AGENT ==========
async def build_agent():
    """
    Build the research agent with web search MCP tools.

    Returns:
        A LangChain agent configured with web search MCP tools.
    """
    return await build_mcp_agent(
        system_prompt=PROMPT,
        mcp_endpoints=[_web_search_mcp_endpoint()],
    )
