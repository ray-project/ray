import asyncio
import os
import time
from typing import Any, Dict, List

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver

from logger_utils import (
    extract_final_text,
    log_final,
    log_model_request,
    log_tool_end,
    log_tool_start,
    truncate_preview,
)

# Optional: Simple dummy tool to verify tool-calling works even if MCP is empty.
# ========== CONFIG ==========
# Easy-to-edit default configurations.
API_KEY = "VrBDo0s-qNOaP9kugBQtJQhGAIA6EUszb6iJHbB1xDQ"
OPENAI_COMPAT_BASE_URL = "https://llm-deploy-qwen-service-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com/v1"  # Make sure to include "/v1".
MODEL = "Qwen/Qwen3-4B-Instruct-2507-FP8"
TEMPERATURE = 0.01
WEATHER_MCP_BASE_URL = "https://weather-mcp-service-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com/mcp"  # Make sure to include "/mcp".
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
    base_url=OPENAI_COMPAT_BASE_URL,
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

# ========== MCP  ==========
# If you're using langchain_mcp_adapters, import and connect here.
# This code logs tools clearly and works even if MCP returns no tools.
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
                    "url": WEATHER_MCP_BASE_URL,
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

# ========== RUN WITH RICH TRACE (EVENTS) ==========
async def run_agent_with_trace(user_request: str, max_iterations: int = 5, show_model_messages: bool = False):
    """Run the agent for a single user request while streaming events for logging.
    Streams model and tool events for structured logging, captures the final
    response text, and returns the number of tool calls used.
    
    Args:
        user_request: The user's input request.
        max_iterations: Maximum number of iterations the agent can run.
        show_model_messages: If True, logs messages sent to the model. If False, hides them.
    """
    agent = await build_agent()

    print("\n" + "#"*60)
    print(f"USER REQUEST: {user_request}")
    print("#"*60)
    print(f"Max iterations: {max_iterations}")
    print(f"Show model messages: {show_model_messages}")

    config = {
        "configurable": {"thread_id": "1"},
        "recursion_limit": max_iterations * 2,
    }
    inputs = {"messages": [{"role": "user", "content": user_request}]}

    tool_calls = 0
    final_text = None

    # Use low-level event stream so we can log everything deterministically.
    async for ev in agent.astream_events(inputs, version="v2", config=config):
        kind = ev["event"]
        
        if kind == "on_chat_model_start":
            # Only log model request if show_model_messages is True
            if show_model_messages:
                input_data = ev["data"].get("input", {})
                messages_raw = input_data.get("messages", [])
                
                # messages_raw is a list containing one list of message objects
                # Unwrap it to get the actual messages
                messages = messages_raw[0] if messages_raw and isinstance(messages_raw[0], list) else messages_raw
                
                log_model_request(
                    name=ev["name"],
                    payload={
                        "messages": messages,
                        "system": PROMPT,
                    },
                )

        # Tool lifecycle.
        elif kind == "on_tool_start":
            name = ev.get("name") or ev["data"].get("name") or "tool"
            args = ev["data"].get("input") or {}
            log_tool_start(name, args)

        elif kind == "on_tool_end":
            name = ev.get("name") or "tool"
            out = ev["data"].get("output")
            log_tool_end(name, truncate_preview(out))
            tool_calls += 1

        # Final output (model end often carries it).
        elif kind in ("on_chain_end", "on_chat_model_end"):
            out = ev["data"].get("output")
            final_text = extract_final_text(out) or final_text

    if final_text is None:
        final_text = "(No final text returned.)"

    log_final(final_text, tool_calls)
    return tool_calls

# ========== MAIN ==========
if __name__ == "__main__":
    # Example request.
    start_time = time.time()
    user_request = "what is the weather like in palo alto?"

    # Set show_model_messages to False to hide the messages sent to model
    asyncio.run(run_agent_with_trace(user_request, max_iterations=5, show_model_messages=True))

    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")