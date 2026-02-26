# SPDX-License-Identifier: Apache-2.0
"""
vLLM plugin that crashes a worker process when a signal file is present.

Monkey-patches the MoE expert-parallel all-to-all primitives used on L4 GPUs:

    AgRsAll2AllManager.dispatch()   — distributes tokens to remote experts
    AgRsAll2AllManager.combine()    — gathers results back from remote experts

These are the allgather-reducescatter all-to-all ops that run inside every
MoE forward pass when expert parallelism is enabled across DP ranks.

When the signal file exists on disk, the next dispatch() or combine() call
kills the worker process with os._exit(1).  This simulates a real NCCL
all-to-all failure and triggers the chain:

    os._exit in worker → RayActorError → EngineCore sets engine_dead
    → AsyncLLM.check_health() raises → Serve controller tears down gang

Activation:
    1. Install this package (pip install .) on all cluster nodes.
    2. Create the signal file  /tmp/a2a_fault_signal  on the target node(s).
    3. The next MoE all-to-all call in any worker on that node will kill the
       worker process immediately.

The plugin always patches when loaded (no env-var gate) because vLLM's
RayDistributedExecutor does not propagate custom env vars to worker actors.
The signal file is the sole activation mechanism.
"""

import os
import logging
from functools import wraps

logger = logging.getLogger(__name__)

SIGNAL_FILE = "/tmp/a2a_fault_signal"

_patched = False


def _make_fault_wrapper(func):
    """Wrap an all-to-all function to crash the process when signaled."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if os.path.exists(SIGNAL_FILE):
            logger.error(
                "Fault signal detected (%s)! Killing worker process "
                "(pid=%d) to simulate NCCL all-to-all failure.",
                SIGNAL_FILE,
                os.getpid(),
            )
            os._exit(1)
        return func(*args, **kwargs)

    return wrapper


def register():
    """vLLM plugin entry point — patches MoE all-to-all ops with fault injection.

    Called by vLLM's plugin loader in every process that imports vllm
    (Serve actor, EngineCore, workers).  Always patches; the actual fault
    is gated by the signal file at runtime.
    """
    global _patched

    if _patched:
        return

    try:
        from vllm.distributed.device_communicators.all2all import (
            AgRsAll2AllManager,
        )

        AgRsAll2AllManager.dispatch = _make_fault_wrapper(
            AgRsAll2AllManager.dispatch
        )
        AgRsAll2AllManager.combine = _make_fault_wrapper(
            AgRsAll2AllManager.combine
        )
        logger.info(
            "vllm_fault plugin: patched AgRsAll2AllManager.dispatch/combine "
            "(pid=%d)",
            os.getpid(),
        )
    except ImportError as e:
        logger.warning("vllm_fault plugin: failed to patch AgRsAll2AllManager: %s", e)

    _patched = True
