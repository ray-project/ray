"""
Travel planning agent that orchestrates by calling two downstream agents over A2A:
  - Research agent (A2A) -> attractions/logistics/sources
  - Weather agent  (A2A) -> forecast/packing suggestions

This agent is itself a LangChain agent so it can be served via the existing
Ray Serve patterns (/chat SSE and/or A2A wrapper).
"""

from __future__ import annotations

import os

from langchain_core.tools import tool

from agent_runtime.agent_builder import build_tool_agent
from protocols.a2a_client import a2a_execute_text


# ========== A2A CONFIG ==========
# Downstream A2A agent base URLs (no trailing slash). These should point at:
#   http://host:8000/a2a-research
#   http://host:8000/a2a-weather
RESEARCH_A2A_BASE_URL = os.getenv(
    "RESEARCH_A2A_BASE_URL", "http://127.0.0.1:8000/a2a-research"
).rstrip("/")
WEATHER_A2A_BASE_URL = os.getenv(
    "WEATHER_A2A_BASE_URL", "http://127.0.0.1:8000/a2a-weather"
).rstrip("/")

# Timeout in seconds for downstream agent calls
# Note: Travel agent calls both research and weather agents which can be slow
# (research does web searches, weather calls external APIs). 360s provides headroom.
A2A_TIMEOUT_S = float(os.getenv("A2A_TIMEOUT_S", "360"))


# ========== A2A TOOLS ==========
@tool
async def a2a_research(query: str) -> str:
    """Call the Research agent over A2A to gather up-to-date info and sources."""
    return await a2a_execute_text(RESEARCH_A2A_BASE_URL, query, timeout_s=A2A_TIMEOUT_S)


@tool
async def a2a_weather(query: str) -> str:
    """Call the Weather agent over A2A to get weather/forecast guidance."""
    return await a2a_execute_text(WEATHER_A2A_BASE_URL, query, timeout_s=A2A_TIMEOUT_S)


# ========== SYSTEM PROMPT ==========
PROMPT = (
    "You are a travel planning assistant.\n"
    "\n"
    "You have TWO tools you must use for every travel-plan request:\n"
    "- a2a_research(query): research attractions/logistics/costs/safety and return sources\n"
    "- a2a_weather(query): get weather/forecast context for the destination and dates\n"
    "\n"
    "Rules:\n"
    "- Always call BOTH tools at least once before producing the final travel plan.\n"
    "- If the user is missing key constraints (dates, budget, origin airport/city, travelers, pace, interests), "
    "ask up to 5 concise clarification questions first.\n"
    "- Otherwise, produce a structured travel plan with:\n"
    "  1) Assumptions & trip summary\n"
    "  2) Day-by-day itinerary (morning/afternoon/evening)\n"
    "  3) Weather-aware packing + timing suggestions\n"
    "  4) Budget outline (high/medium/low)\n"
    "  5) Bookings checklist + local transit notes\n"
    "  6) Sources (from research tool output)\n"
    "- Be practical and specific; avoid vague filler.\n"
)


# ========== BUILD AGENT ==========
async def build_agent():
    """
    Build the travel planning agent with A2A tools.

    Returns:
        A LangChain agent configured with A2A research and weather tools.
    """
    tools = [a2a_research, a2a_weather]
    return await build_tool_agent(
        system_prompt=PROMPT,
        tools=tools,
        temperature=0.2,
    )
