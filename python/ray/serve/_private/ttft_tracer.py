import json
import logging
import os
import time
from typing import Optional

# Environment variable to enable/disable TTFT tracing (enabled by default)
TTFT_TRACING_ENABLED = os.getenv("TTFT_TRACING", "1").lower() in ("1", "true", "yes")

# Get a dedicated logger for TTFT tracing
ttft_logger = logging.getLogger("ttft_tracer")
ttft_logger.setLevel(logging.INFO)

# Function to ensure logger is set up (called lazily)
def _setup_logger():
    if TTFT_TRACING_ENABLED and not ttft_logger.handlers:
        # Write to a file in shared storage or temp directory
        log_file = os.path.expanduser(
            os.getenv("TTFT_LOG_FILE", "~/default/ttft_trace.log")
        )

        # Ensure directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # Create a file handler for TTFT logs
        handler = logging.FileHandler(log_file, mode="a")
        # Use simple format to avoid timestamp duplication since we include ts_ns
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        ttft_logger.addHandler(handler)
        ttft_logger.propagate = False

        # Debug: Write initial marker to confirm logging is working
        ttft_logger.info(
            f"# TTFT tracing started at {time.time_ns()}, logging to {log_file}"
        )


def log_ttft_event(
    step: str,
    event: str,
    request_id: str,
    function_name: Optional[str] = None,
    prompt_len: Optional[int] = None,
    model_id: Optional[str] = None,
    queued_and_ongoing_reqs: Optional[int] = None,
) -> None:
    """
    Log a TTFT timing event.

    Args:
        step: Stage of processing ("router", "replica", "engine")
        event: Event type ("rx" for received, "tx" for transmitted/forwarded)
        request_id: Unique identifier for the request
        function_name: Optional name of the function where this event occurs
        prompt_len: Optional prompt length for analysis
        model_id: Optional model identifier
        queued_and_ongoing_reqs: Optional number of queued and ongoing requests for replica load analysis
    """
    if not TTFT_TRACING_ENABLED:
        return

    # Ensure logger is set up on first use
    _setup_logger()

    event_data = {
        "step": step,
        "event": event,
        "ts_ns": time.time_ns(),
        "request_id": request_id,
    }

    if function_name is not None:
        event_data["function_name"] = function_name

    if prompt_len is not None:
        event_data["prompt_len"] = prompt_len

    if model_id is not None:
        event_data["model_id"] = model_id

    if queued_and_ongoing_reqs is not None:
        event_data["queued_and_ongoing_reqs"] = queued_and_ongoing_reqs

    # Log as JSON without extra formatting
    ttft_logger.info(json.dumps(event_data, separators=(",", ":")))


def get_request_id_from_metadata(metadata) -> str:
    """
    Extract request ID from various metadata types.

    Args:
        metadata: Request metadata object

    Returns:
        Request ID string
    """
    if hasattr(metadata, "request_id"):
        return metadata.request_id
    elif hasattr(metadata, "getRequestId"):
        return metadata.getRequestId()
    elif isinstance(metadata, dict) and "request_id" in metadata:
        return metadata["request_id"]
    else:
        # Fallback - try to get string representation
        return str(metadata)


def is_tracing_enabled() -> bool:
    """Check if TTFT tracing is enabled."""
    return TTFT_TRACING_ENABLED
