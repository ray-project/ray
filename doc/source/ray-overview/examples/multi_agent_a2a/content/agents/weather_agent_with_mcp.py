"""
Weather agent that uses an MCP server to answer weather questions.

This agent connects to a Weather MCP server and uses the discovered tools
to provide weather information.
"""

from __future__ import annotations

import os

from agent_runtime.agent_builder import build_mcp_agent
from agent_runtime.config import MCPEndpoint


# ========== MCP CONFIG ==========
# Weather MCP server endpoint configuration
WEATHER_MCP_BASE_URL = os.getenv("WEATHER_MCP_BASE_URL", "").strip().rstrip("/")
WEATHER_MCP_TOKEN = os.getenv("WEATHER_MCP_TOKEN", "").strip()


def _weather_mcp_endpoint() -> MCPEndpoint:
    """Build Weather MCP endpoint configuration."""
    return MCPEndpoint(
        name="weather",
        base_url=WEATHER_MCP_BASE_URL,
        token=WEATHER_MCP_TOKEN,
    )


# ========== SYSTEM PROMPT ==========
PROMPT = (
    "You are a weather assistant that provides accurate weather information "
    "using available tools.\n"
    "\n"
    "Follow this process:\n"
    "- Break tasks into sub-questions (e.g., finding coordinates first).\n"
    "- Use the weather tools to get current conditions and forecasts.\n"
    "- Provide a concise, actionable answer for the user.\n"
    "\n"
    "Only output final answers or tool calls (no hidden thoughts)."
)


# ========== BUILD AGENT ==========
async def build_agent():
    """
    Build the weather agent with MCP tools.

    Returns:
        A LangChain agent configured with weather MCP tools.
    """
    return await build_mcp_agent(
        system_prompt=PROMPT,
        mcp_endpoints=[_weather_mcp_endpoint()],
    )
