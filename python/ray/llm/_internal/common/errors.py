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
