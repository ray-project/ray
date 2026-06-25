import json
from types import SimpleNamespace
from typing import Optional


def parse_routing_payload(body: bytes) -> Optional[SimpleNamespace]:
    """Derive a routing key from a request body.

    Body-aware routers read ``messages`` or ``prompt`` off the first positional
    routing arg, where the OpenAI ingress puts the parsed request. Direct
    streaming has only the raw body, so this exposes the present field on a
    plain object the routers read the same way. Uses ``json.loads`` rather than
    a full parse. Returns ``None`` for an empty, non-object, unparseable, or
    fieldless body.
    """
    if not body:
        return None
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    # Keep only the non-empty routing fields. Empty messages or prompt carry no
    # routing signal, and exposing just the present field lets ``hasattr`` tell
    # a chat body from a completion body.
    key = {field: data[field] for field in ("messages", "prompt") if data.get(field)}
    if not key:
        return None
    return SimpleNamespace(**key)
