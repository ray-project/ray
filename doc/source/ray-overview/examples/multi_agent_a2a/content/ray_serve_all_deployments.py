"""
Unified Ray Serve deployments for all agents and services.

This file consolidates:
- Standard agent deployments (streaming /chat endpoints)
- A2A-wrapped agents (discovery + execute endpoints)
- Multi-app deployment entrypoint

Services:
- /llm               -> OpenAI-compatible LLM API
- /mcp-web-search    -> MCP web search server
- /mcp-weather       -> MCP weather server
- /weather-agent     -> Weather agent (uses weather MCP)
- /research-agent    -> Research agent (uses web-search MCP)
- /travel-agent      -> Travel agent (calls research + weather agents via A2A)
- /a2a-weather       -> A2A-wrapped weather agent
- /a2a-research      -> A2A-wrapped research agent
- /a2a-travel        -> A2A-wrapped travel agent

Usage:
    # Deploy individual agents:
    serve run ray_serve_all_deployments:weather_app
    serve run ray_serve_all_deployments:research_app
    serve run ray_serve_all_deployments:travel_app

    # Deploy A2A-wrapped agents:
    serve run ray_serve_all_deployments:a2a_weather_app
    serve run ray_serve_all_deployments:a2a_research_app
    serve run ray_serve_all_deployments:a2a_travel_app

    # Deploy all services:
    serve run serve_multi_config.yaml
"""

from __future__ import annotations

from protocols.a2a_card import build_agent_card
from agent_runtime.a2a_deployment import create_a2a_deployment
from agent_runtime.serve_deployment import create_agent_deployment


# ============================================================
# LLM and MCP Server Imports (for multi-app deployment)
# ============================================================

from mcps.web_search_mcp_server import app as web_search_mcp_app
from mcps.weather_mcp_server import app as weather_mcp_app


# ============================================================
# Agent Cards (A2A Discovery Metadata)
# ============================================================

WEATHER_CARD = build_agent_card(
    name="weather-agent",
    description="Weather agent that uses a Weather MCP server to answer weather questions.",
    version="0.1.0",
    skills=["weather", "forecast", "current_conditions"],
)

RESEARCH_CARD = build_agent_card(
    name="research-agent",
    description="Research agent that uses a Web Search MCP server to gather sources and summarize.",
    version="0.1.0",
    skills=["web_search", "research", "summarization", "fact_checking"],
)

TRAVEL_CARD = build_agent_card(
    name="travel-agent",
    description=(
        "Travel planning agent that calls the Research and Weather agents over A2A "
        "to produce a structured, weather-aware itinerary."
    ),
    version="0.1.0",
    skills=["travel_planning", "itinerary", "weather_aware", "logistics"],
)


# ============================================================
# Agent Builder Functions (Lazy Imports)
# ============================================================

def _build_weather_agent():
    """Lazily import and build the weather agent."""
    from agents.weather_agent_with_mcp import build_agent
    return build_agent()


def _build_research_agent():
    """Lazily import and build the research agent."""
    from agents.research_agent_with_web_search_mcp import build_agent
    return build_agent()


def _build_travel_agent():
    """Lazily import and build the travel agent."""
    from agents.travel_agent_with_a2a import build_agent
    return build_agent()


# ============================================================
# Standard Agent Deployments (streaming /chat endpoints)
# ============================================================

weather_agent_app = create_agent_deployment(
    _build_weather_agent,
    name="WeatherAgentDeployment",
    title="Weather Agent",
    description="Weather agent that uses a Weather MCP server to answer weather questions.",
)

research_agent_app = create_agent_deployment(
    _build_research_agent,
    name="ResearchAgentDeployment",
    title="Research Agent",
    description="Research agent that uses a Web Search MCP server to gather sources and summarize.",
)

travel_agent_app = create_agent_deployment(
    _build_travel_agent,
    name="TravelAgentDeployment",
    title="Travel Agent",
    description="Travel planning agent that orchestrates Research and Weather agents via A2A.",
)


# ============================================================
# A2A-Wrapped Agent Deployments (discovery + execute endpoints)
# ============================================================

a2a_weather_app = create_a2a_deployment(_build_weather_agent, WEATHER_CARD)
a2a_research_app = create_a2a_deployment(_build_research_agent, RESEARCH_CARD)
a2a_travel_app = create_a2a_deployment(_build_travel_agent, TRAVEL_CARD)

