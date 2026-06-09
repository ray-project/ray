"""Base interface every framework adapter implements.

An adapter owns the framework-specific bits (how to build the model, the
distributed engine, and run one optimizer step) while the harness owns
config, metrics, checkpointing cadence, and launching. The contract is
deliberately small so adding a framework (torchtitan, maxtext, megatron) is a
single new ``adapter.py``.
"""

from abc import ABC, abstractmethod
from typing import Optional

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
    def run(self) -> None:
        """Run the full training loop, reporting metrics via self.ctx."""
