"""Base interface every framework adapter implements.

An adapter owns the framework-specific bits (how to build the model, the
distributed engine, and run one optimizer step) while the harness owns
config, metrics, checkpointing cadence, and launching. The contract is
deliberately small so adding a framework (torchtitan, maxtext, megatron) is a
single new ``adapter.py``.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from core.experiment_config import ExperimentConfig
from core.train_context import TrainContext


class FrameworkAdapter(ABC):
    def __init__(self, cfg: ExperimentConfig, ctx: TrainContext):
        self.cfg = cfg
        self.ctx = ctx

    @abstractmethod
    def flops_per_token(self) -> Optional[float]:
        """Model FLOPs per token for MFU; None if not estimable."""

    @abstractmethod
    def run(self) -> Dict[str, Any]:
        """Run the full training loop, report metrics via ``self.ctx``, and
        RETURN the final metrics dict (same across data-parallel ranks).

        The return value is required, not optional: the torchrun_ray launcher
        collects metrics from the returned value (and selects rank 0's). The Ray
        Train launcher instead reads them off the Result via ``report``, but
        adapters must still return so both launchers work unchanged.
        """
