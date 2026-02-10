"""Agent runner with trace logging for debugging and monitoring."""
from typing import Any

from logger_utils import (
    extract_final_text,
    log_final,
    log_model_request,
    log_tool_end,
    log_tool_start,
    truncate_preview,
)


async def run_agent_with_trace(
    agent: Any,
    user_request: str,
    system_prompt: str,
    max_iterations: int = 5,
    show_model_messages: bool = False,
) -> int:
    """Run the agent for a single user request while streaming events for logging.

    Streams model and tool events for structured logging, captures the final
    response text, and returns the number of tool calls used.

    Args:
        agent: The agent instance to run.
        user_request: The user's input request.
        system_prompt: The system prompt used by the agent.
        max_iterations: Maximum number of iterations the agent can run.
        show_model_messages: If True, logs messages sent to the model. If False, hides them.

    Returns:
        The number of tool calls made during execution.
    """
    print("\n" + "#" * 60)
    print(f"USER REQUEST: {user_request}")
    print("#" * 60)
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
                messages = (
                    messages_raw[0]
                    if messages_raw and isinstance(messages_raw[0], list)
                    else messages_raw
                )

                log_model_request(
                    name=ev["name"],
                    payload={
                        "messages": messages,
                        "system": system_prompt,
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
