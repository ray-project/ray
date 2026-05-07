"""
Shared agent-building helpers.

This repo had multiple agents with very similar boilerplate:
- load config
- build LLM
- (optionally) discover MCP tools
- create a LangChain agent with MemorySaver

This module centralizes that logic so individual agents only specify:
- system prompt
- tools source (MCP endpoints vs explicitly provided tools)
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver

from agent_runtime.config import LLMConfig, MCPEndpoint, load_llm_config


# =============================================================================
# LLM Builder
# =============================================================================


def build_llm(config: LLMConfig, *, streaming: bool = False) -> ChatOpenAI:
    """
    Build a ChatOpenAI instance from LLM configuration.

    Args:
        config: LLM configuration with model settings
        streaming: Whether to enable streaming mode

    Returns:
        Configured ChatOpenAI instance

    Raises:
        ValueError: If required configuration is missing
    """
    if not config.openai_base_url:
        raise ValueError(
            "OPENAI_COMPAT_BASE_URL is required (base URL for your OpenAI-compatible LLM service)."
        )
    if not config.openai_api_key:
        raise ValueError("OPENAI_API_KEY is required.")

    # Build headers if Anyscale version is specified
    default_headers = {}
    if config.anyscale_version:
        default_headers["X-ANYSCALE-VERSION"] = config.anyscale_version

    # NOTE: urllib.parse.urljoin will DROP the last path segment if the base URL
    # does not end with "/". For example:
    #   urljoin("http://127.0.0.1:8000/llm", "v1") -> "http://127.0.0.1:8000/v1"
    # We always force a trailing slash so "/llm/" + "v1" -> "/llm/v1".
    base_url = urljoin(config.openai_base_url.rstrip("/") + "/", "v1")

    return ChatOpenAI(
        model=config.model,
        base_url=base_url,
        api_key=config.openai_api_key,
        temperature=config.temperature,
        streaming=streaming,
        default_headers=default_headers or None,
    )


# =============================================================================
# MCP Tools Discovery
# =============================================================================


async def load_mcp_tools(
    endpoints: List[MCPEndpoint],
    *,
    anyscale_version: str = "",
) -> List[Any]:
    """
    Load tools from one or more MCP server endpoints.

    Args:
        endpoints: List of MCPEndpoint configs (uses endpoint.name as server key)
        anyscale_version: Optional Anyscale version header

    Returns:
        List of discovered LangChain tools from all MCP servers

    Example:
        >>> endpoint = MCPEndpoint(name="weather", base_url="http://...")
        >>> tools = await load_mcp_tools([endpoint])
    """
    if not endpoints:
        print("[MCP] No MCP endpoints configured; continuing without MCP tools.")
        return []

    try:
        from langchain_mcp_adapters.client import MultiServerMCPClient

        # Build server configurations
        servers: Dict[str, Dict[str, Any]] = {}
        for endpoint in endpoints:
            if not endpoint.base_url:
                print(f"[MCP] Skipping {endpoint.name}: no base URL configured")
                continue

            headers = {}
            if endpoint.token:
                headers["Authorization"] = f"Bearer {endpoint.token}"
            if anyscale_version:
                headers["X-ANYSCALE-VERSION"] = anyscale_version

            # Same urljoin caveat: ensure trailing slash before joining with "mcp"
            servers[endpoint.name] = {
                "url": urljoin(endpoint.base_url.rstrip("/") + "/", "mcp"),
                "transport": "streamable_http",
                "headers": headers,
            }

        if not servers:
            print("[MCP] No valid MCP servers configured.")
            return []

        mcp_client = MultiServerMCPClient(servers)
        tools = await mcp_client.get_tools()

        print(f"\n[MCP] Discovered {len(tools)} tool(s) from {len(servers)} MCP server(s).")
        for tool_obj in tools:
            name = getattr(tool_obj, "name", type(tool_obj).__name__)
            desc = getattr(tool_obj, "description", "") or ""
            print(f"  - {name}: {desc[:120]}")

        return tools

    except Exception as exc:
        print(f"[MCP] Skipping MCP tools (error): {exc}")
        return []


# =============================================================================
# Agent Builders
# =============================================================================


async def build_tool_agent(
    *,
    system_prompt: str,
    tools: Iterable[Any],
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    llm_config: Optional[LLMConfig] = None,
) -> Any:
    """
    Build a LangChain agent with an explicit tool list.

    Args:
        system_prompt: System prompt for the agent
        tools: List of tools to provide to the agent
        model: Override model name (if llm_config not provided)
        temperature: Override temperature (if llm_config not provided)
        llm_config: LLM configuration (takes precedence over model/temperature)

    Returns:
        Configured LangChain agent
    """
    cfg = llm_config or load_llm_config(model=model, temperature=temperature)
    llm = build_llm(cfg)
    memory = MemorySaver()
    return create_agent(
        llm,
        list(tools),
        system_prompt=system_prompt,
        checkpointer=memory,
    )


async def build_mcp_agent(
    *,
    system_prompt: str,
    mcp_endpoints: List[MCPEndpoint],
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    llm_config: Optional[LLMConfig] = None,
) -> Any:
    """
    Build a LangChain agent whose tools are discovered from one or more MCP servers.

    Args:
        system_prompt: System prompt for the agent
        mcp_endpoints: List of MCPEndpoint configs (uses endpoint.name as server key)
        model: Override model name (if llm_config not provided)
        temperature: Override temperature (if llm_config not provided)
        llm_config: LLM configuration (takes precedence over model/temperature)

    Returns:
        Configured LangChain agent with MCP tools
    """
    cfg = llm_config or load_llm_config(model=model, temperature=temperature)
    llm = build_llm(cfg)
    mcp_tools = await load_mcp_tools(mcp_endpoints, anyscale_version=cfg.anyscale_version)
    memory = MemorySaver()
    return create_agent(
        llm,
        list(mcp_tools),
        system_prompt=system_prompt,
        checkpointer=memory,
    )
