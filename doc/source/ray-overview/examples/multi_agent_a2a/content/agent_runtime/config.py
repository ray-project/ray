"""
Centralized configuration loading for all agents.

LLM configuration is separate from MCP endpoint configuration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class LLMConfig:
    """Configuration for the LLM backend."""

    model: str = "Qwen/Qwen3-4B-Instruct-2507-FP8"
    temperature: float = 0.01
    openai_base_url: str = ""
    openai_api_key: str = ""
    anyscale_version: str = ""

    def __post_init__(self):
        # Ensure base URL doesn't have trailing slash for consistent urljoin behavior
        self.openai_base_url = self.openai_base_url.rstrip("/")


@dataclass
class MCPEndpoint:
    """Configuration for an MCP server endpoint."""

    base_url: str
    token: str = ""
    name: str = "mcp"  # Server name for MultiServerMCPClient

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")


# Default values (non-sensitive only)
_DEFAULTS = {
    "model": "Qwen/Qwen3-4B-Instruct-2507-FP8",
    "temperature": "0.01",
}


def load_llm_config(
    *,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
) -> LLMConfig:
    """
    Load LLM configuration from environment variables with sensible defaults.

    Environment variables:
    - MODEL: LLM model name
    - TEMPERATURE: LLM temperature (0.0-1.0)
    - OPENAI_COMPAT_BASE_URL: Base URL for OpenAI-compatible LLM service
    - OPENAI_API_KEY: API key for the LLM service
    - X_ANYSCALE_VERSION or ANYSCALE_VERSION: Optional version header

    Args:
        model: Override model name (takes precedence over env var)
        temperature: Override temperature (takes precedence over env var)

    Returns:
        LLMConfig instance with loaded values
    """
    return LLMConfig(
        model=model or os.getenv("MODEL", _DEFAULTS["model"]),
        temperature=temperature if temperature is not None else float(os.getenv("TEMPERATURE", _DEFAULTS["temperature"])),
        openai_base_url=os.getenv("OPENAI_COMPAT_BASE_URL", "").strip(),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        anyscale_version=os.getenv(
            "X_ANYSCALE_VERSION",
            os.getenv("ANYSCALE_VERSION", ""),
        ).strip(),
    )
