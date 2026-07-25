"""Re-export of the Serve-core ResponseChannel.

The ResponseChannel is a general Serve primitive (any deployment can stream its
response back to HAProxy, off the parents' response path). It lives in Serve
core; this module re-exports it for the LLM serving code.
"""

from ray.serve._private.response_channel import (  # noqa: F401
    RESPONSE_ID_HEADER,
    ResponseChannel,
    _to_json_line,
    haproxy_base_for_leaf,
)
