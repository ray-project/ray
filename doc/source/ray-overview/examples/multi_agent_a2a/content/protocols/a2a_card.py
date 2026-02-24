"""
Helpers for creating A2A Agent Cards using the official `a2a-sdk`.

This repo previously used a local "minimal protocol" implementation. We now
use the canonical models from `a2a.types`.
"""

from __future__ import annotations

import os
from typing import Iterable, Sequence

from a2a.types import AgentCapabilities, AgentCard, AgentSkill, TransportProtocol


def build_agent_card(
    *,
    name: str,
    description: str,
    version: str = "0.1.0",
    skills: Sequence[str] | None = None,
    url: str | None = None,
    documentation_url: str | None = None,
    icon_url: str | None = None,
) -> AgentCard:
    """
    Build an `a2a.types.AgentCard` for an agent served over HTTP+JSON (REST).

    Notes:
    - `AgentCard.url` should be the *base URL* for the agent (no trailing slash),
      e.g. "http://127.0.0.1:8000/a2a-weather". The REST endpoints are mounted
      under that base (e.g. `/v1/message:send`).
    - If `url` is omitted, we fall back to `A2A_AGENT_URL` env var.
    """
    base_url = (url or os.getenv("A2A_AGENT_URL", "")).strip().rstrip("/")
    if not base_url:
        # Keep a deterministic placeholder so the card validates.
        # In Ray Serve deployments, prefer setting A2A_AGENT_URL per application.
        base_url = "http://127.0.0.1:8000"

    tags = [s for s in (skills or []) if isinstance(s, str) and s.strip()]
    tags = list(dict.fromkeys([t.strip() for t in tags]))  # de-dupe, preserve order

    # Keep the card simple: one "primary" skill that advertises tags.
    primary_skill = AgentSkill(
        id=f"{name}-primary",
        name=name,
        description=description,
        tags=tags or [name],
    )

    return AgentCard(
        name=name,
        description=description,
        version=version,
        url=base_url,
        preferred_transport=TransportProtocol.http_json.value,
        documentation_url=documentation_url,
        icon_url=icon_url,
        capabilities=AgentCapabilities(
            streaming=True,
            push_notifications=False,
            state_transition_history=False,
        ),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[primary_skill],
    )
