from typing import Dict, Optional

from ray.anyscale.serve._private.tracing_utils import (
    get_trace_context as private_get_trace_context,
)


def get_trace_context() -> Optional[Dict[str, str]]:
    return private_get_trace_context()
