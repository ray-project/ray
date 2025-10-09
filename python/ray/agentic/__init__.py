"""
Ray Agentic: Distributed agent orchestration on Ray.

This package provides primitives for running LLM agents (LangGraph, CrewAI, etc.)
with distributed tool execution across Ray clusters.

Currently, only the experimental API is available. See `ray.agentic.experimental`
for the developer API.

Example:
    >>> import ray
    >>> from ray.agentic.experimental import AgentSession
    >>> from ray.agentic.experimental.adapters import LangGraphAdapter
    >>>
    >>> session = AgentSession.remote("user_123", LangGraphAdapter())
    >>> result = ray.get(session.run.remote("Hello"))
"""

# Top-level package - currently only experimental submodule
# No imports here to avoid accidental API exposure

__version__ = "0.1.0"
