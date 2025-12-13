import asyncio
import os
import time
from typing import Any, Dict, List
from urllib.parse import urljoin

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver

# ========== CONFIG ==========
# Easy-to-edit default configurations.
API_KEY = "VrBDo0s-qNOaP9kugBQtJQhGAIA6EUszb6iJHbB1xDQ"
OPENAI_COMPAT_BASE_URL = (
    "https://llm-deploy-qwen-service-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com"
)
MODEL = "Qwen/Qwen3-4B-Instruct-2507-FP8"
TEMPERATURE = 0.01
WEATHER_MCP_BASE_URL = (
    "https://weather-mcp-service-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com"
)
WEATHER_MCP_TOKEN = "uyOArxwCNeTpxn0odOW7hGY57tXQNNrF16Yy8ziskrY"

# Environment variable overrides.
# For deployment, you can override the above settings with environment variables.
API_KEY = os.getenv("OPENAI_API_KEY", API_KEY)
OPENAI_COMPAT_BASE_URL = os.getenv("OPENAI_COMPAT_BASE_URL", OPENAI_COMPAT_BASE_URL)
WEATHER_MCP_BASE_URL = os.getenv("WEATHER_MCP_BASE_URL", WEATHER_MCP_BASE_URL)
WEATHER_MCP_TOKEN = os.getenv("WEATHER_MCP_TOKEN", WEATHER_MCP_TOKEN)


# ========== LLM ==========
llm = ChatOpenAI(
    model=MODEL,
    base_url=urljoin(
        OPENAI_COMPAT_BASE_URL, "v1"
    ),  ## urljoin automatically appends "/v1" to the base URL.
    api_key=API_KEY,
    temperature=TEMPERATURE,
    streaming=False,
)


# ========== SYSTEM PROMPT ==========
PROMPT = (
    "You are a research assistant that uses multiple tool calls to gather comprehensive information.\n"
    "\n"
    "Follow this process:\n"
    "- Break tasks into sub-questions.\n"
    "- Prefer calling tools when information is external or time-sensitive.\n"
    "- After each tool call, decide whether more calls are needed.\n"
    "- When sufficient, provide a concise, actionable answer.\n"
    "\n"
    "Only output final answers or tool calls (no hidden thoughts)."
)

# ========== MCP Tools  ==========
async def get_mcp_tools() -> List[Any]:
    """Return tools discovered from the configured MCP server."""
    try:
        from langchain_mcp_adapters.client import MultiServerMCPClient

        headers = {
            "Authorization": f"Bearer {WEATHER_MCP_TOKEN}",
        }
        mcp_client = MultiServerMCPClient(
            {
                "weather": {
                    "url": urljoin(
                        WEATHER_MCP_BASE_URL, "mcp"
                    ),  ## urljoin automatically appends "/mcp" to the base URL.
                    "transport": "streamable_http",
                    "headers": headers,
                }
            }
        )

        tools = await mcp_client.get_tools()
        print(f"\n[MCP] Discovered {len(tools)} tool(s) from MCP.")
        for tool_obj in tools:
            # Most MCP adapters expose .name and .description on LangChain tools.
            name = getattr(tool_obj, "name", type(tool_obj).__name__)
            desc = getattr(tool_obj, "description", "") or ""
            print(f"  - {name}: {desc[:120]}")
        return tools
    except Exception as exc:
        print(f"[MCP] Skipping MCP tools (error): {exc}")
        return []


# ========== BUILD AGENT ==========
async def build_agent():
    """Instantiate an agent with MCP tools when available."""
    mcp_tools = await get_mcp_tools()

    tools = list(mcp_tools)
    print(f"\n[Agent] Using {len(tools)} tool(s).")

    memory = MemorySaver()
    agent = create_agent(
        llm,
        tools,
        system_prompt=PROMPT,
        checkpointer=memory,
    )
    return agent


# ========== MAIN ==========
if __name__ == "__main__":
    from helpers.agent_runner import run_agent_with_trace

    async def main():
        # Example request.
        start_time = time.time()
        user_request = "what is the weather like in palo alto?"

        # Build the agent
        agent = await build_agent()

        # Set show_model_messages to False to hide the messages sent to model
        await run_agent_with_trace(
            agent=agent,
            user_request=user_request,
            system_prompt=PROMPT,
            max_iterations=5,
            show_model_messages=True,
        )

        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")

    asyncio.run(main())
