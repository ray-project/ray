"""Fatal engine error definitions shared by serve and batch layers."""

from typing import Tuple, Type

# vLLM fatal errors that should always be re-raised, never swallowed.
# EngineDeadError indicates the vLLM engine process has crashed and is
# unrecoverable — all subsequent requests would fail anyway.
VLLM_FATAL_ERRORS: Tuple[Type[Exception], ...] = ()
try:
    from vllm.v1.engine.exceptions import EngineDeadError

    VLLM_FATAL_ERRORS = (EngineDeadError,)
except ImportError:
    pass

# vLLM errors caused by the request itself. They derive from ``VLLMError``, not
# ``ValueError``, so pydantic leaves them alone during request parsing and they would
# otherwise be reported as 500s.
VLLM_CLIENT_ERRORS: Tuple[Type[Exception], ...] = ()
try:
    from vllm.exceptions import VLLMClientError

    VLLM_CLIENT_ERRORS = (VLLMClientError,)
except ImportError:
    pass
