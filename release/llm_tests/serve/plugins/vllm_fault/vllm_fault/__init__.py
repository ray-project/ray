# SPDX-License-Identifier: Apache-2.0
"""
vLLM plugin that hangs a worker process when signaled via a Ray named actor.

Monkey-patches the MoE expert-parallel all-to-all primitives:

    AgRsAll2AllManager.dispatch()   — distributes tokens to remote experts
    AgRsAll2AllManager.combine()    — gathers results back from remote experts

When the ``fault_signal`` named Ray actor is armed for a specific node,
the first worker on that node to call dispatch() or combine() atomically
claims the fault (via the actor) and hangs forever. Only one worker per
node hangs; the peer worker proceeds normally into NCCL, but the compiled
graph eventually times out because the hung worker never produces output.

The plugin also patches torch.distributed.init_process_group and new_group
to use a short NCCL timeout.

Activation:
    1. Install this package (pip install .) on all cluster nodes.
    2. Create a FaultSignal Ray actor named fault_signal.
    3. Call signal.arm.remote(target_node_id) to arm the fault.
    4. The next MoE all-to-all call on a worker on that node will hang.
"""

import logging
import os
import time
from datetime import timedelta
from functools import wraps

import ray
import torch.distributed as dist
import torch.distributed.distributed_c10d as c10d
from vllm.distributed.device_communicators.all2all import AgRsAll2AllManager

logger = logging.getLogger(__name__)

FAULT_SIGNAL_ACTOR_NAME = "fault_signal"
FAULT_SIGNAL_NAMESPACE = "fault_test"
NCCL_TIMEOUT_S = int(os.environ.get("VLLM_FAULT_NCCL_TIMEOUT_S", "10"))

_patched = False
_cached_signal = None
_skip_check = False


def _make_fault_wrapper(func):
    """Wrap an all-to-all function to hang when signaled via Ray actor."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        global _cached_signal, _skip_check

        if _skip_check:
            return func(*args, **kwargs)

        try:
            if _cached_signal is None:
                _cached_signal = ray.get_actor(
                    FAULT_SIGNAL_ACTOR_NAME,
                    namespace=FAULT_SIGNAL_NAMESPACE,
                )
                logger.warning(
                    "vllm_fault: found fault_signal actor (pid=%d)",
                    os.getpid(),
                )

            node_id = ray.get_runtime_context().get_node_id()
            should_hang, stop_checking = ray.get(
                _cached_signal.check_and_claim.remote(node_id)
            )

            if should_hang:
                logger.error(
                    "Fault signal received! Hanging worker process "
                    "(pid=%d) to simulate NCCL all-to-all hang.",
                    os.getpid(),
                )
                # Hang forever -- The compiled graph will timeout waiting for
                # this worker's output, propagating the error to EngineCore.
                while True:
                    time.sleep(3600)

            if stop_checking:
                _skip_check = True

        except ValueError:
            pass  # Signal actor not created yet — no fault armed
        except Exception:
            _cached_signal = None  # Reset cache on transient errors

        return func(*args, **kwargs)

    return wrapper


def register():
    """
    vLLM plugin entry point — patches NCCL timeout and MoE all-to-all ops.

    Called by vLLM's plugin loader in every process that imports vLLM.
    """
    global _patched

    if _patched:
        return

    # Set a short NCCL timeout for torch.distributed process groups.
    try:
        short_timeout = timedelta(seconds=NCCL_TIMEOUT_S)

        c10d._DEFAULT_PG_TIMEOUT = short_timeout

        _orig_init_pg = dist.init_process_group
        _orig_new_group = dist.new_group

        @wraps(_orig_init_pg)
        def _patched_init_pg(*args, **kwargs):
            if kwargs.get("timeout") is None:
                kwargs["timeout"] = short_timeout
            return _orig_init_pg(*args, **kwargs)

        @wraps(_orig_new_group)
        def _patched_new_group(*args, **kwargs):
            if kwargs.get("timeout") is None:
                kwargs["timeout"] = short_timeout
            return _orig_new_group(*args, **kwargs)

        dist.init_process_group = _patched_init_pg
        dist.new_group = _patched_new_group
        logger.info(
            "vllm_fault plugin: set NCCL timeout to %ds (pid=%d)",
            NCCL_TIMEOUT_S,
            os.getpid(),
        )
    except (ImportError, AttributeError) as e:
        logger.warning("vllm_fault plugin: failed to set NCCL timeout: %s", e)

    # Patch MoE all-to-all ops with fault injection.
    try:
        AgRsAll2AllManager.dispatch = _make_fault_wrapper(AgRsAll2AllManager.dispatch)
        AgRsAll2AllManager.combine = _make_fault_wrapper(AgRsAll2AllManager.combine)
        logger.info(
            "vllm_fault plugin: patched AgRsAll2AllManager.dispatch/combine "
            "(pid=%d)",
            os.getpid(),
        )
    except ImportError as e:
        logger.warning("vllm_fault plugin: failed to patch AgRsAll2AllManager: %s", e)

    _patched = True
