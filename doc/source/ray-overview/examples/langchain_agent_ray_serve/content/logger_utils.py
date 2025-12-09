import json
from typing import Any, Dict, Optional, Sequence, Union

PREVIEW_LIMIT = 800


def _pp(obj: Any) -> str:
    """Pretty-print helper for logging."""
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)


def _iter_messages(payload: Dict[str, Any]) -> Sequence[Any]:
    """Return an iterable of messages from a LangChain payload."""
    messages = payload.get("messages")
    if messages:
        return messages
    inputs = payload.get("inputs", {})
    return inputs.get("messages", []) if isinstance(inputs, dict) else []


def log_model_request(name: str, payload: Dict[str, Any]) -> None:
    """Print the payload sent to the chat model."""
    print("\n" + "=" * 70)
    print(f"MODEL CALL → {name}")
    print("=" * 70)

    messages = _iter_messages(payload)
    if messages:
        print("\n-- Messages sent to model --")
        for message in messages:
            role = getattr(message, "type", getattr(message, "role", ""))
            content = getattr(message, "content", "")
            print(f"[{role}] {content}")

    system = payload.get("system") or payload.get("system_prompt")
    if system:
        print("\n-- System prompt --")
        print(system)


def log_tool_start(name: str, args: Dict[str, Any]) -> None:
    """Print diagnostic information when a tool call starts."""
    print("\n" + "-" * 70)
    print(f"TOOL START → {name}")
    print("-" * 70)
    print(_pp(args))


def log_tool_end(name: str, result_preview: str) -> None:
    """Print diagnostic information when a tool call ends."""
    print("\n" + "-" * 70)
    print(f"TOOL END   ← {name}")
    print("-" * 70)
    print(result_preview)


def log_final(text: str, tool_calls_count: int) -> None:
    """Print the final model response plus summary statistics."""
    print("\n" + "=" * 70)
    print("FINAL RESPONSE")
    print("=" * 70)
    print(text)
    print("\n" + "#" * 60)
    print(f"COMPLETED - Total tool calls made: {tool_calls_count}")
    print("#" * 60 + "\n")


def truncate_preview(text: Union[str, Any], limit: int = PREVIEW_LIMIT) -> str:
    """Shorten long tool outputs to keep logs readable."""
    if isinstance(text, str) and len(text) > limit:
        return text[:limit] + f"... (len={len(text)})"
    return str(text)


def extract_final_text(output: Any) -> Optional[str]:
    """Extract the final string content from LangChain outputs."""
    if isinstance(output, dict):
        messages = output.get("messages")
        if messages:
            last_message = messages[-1]
            return getattr(last_message, "content", None)
    if isinstance(output, list) and output:
        last_entry = output[-1]
        return getattr(last_entry, "content", None)
    if isinstance(output, str):
        return output
    return None
