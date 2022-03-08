from typing import Any, Optional

from ray.ml.checkpoint import Checkpoint


class Result:
    """Result interface."""

    checkpoint: Optional[Checkpoint]
    metrics: Any

    def __init__(self, metrics: Any, checkpoint: Optional[Checkpoint]):
        self.metrics = metrics
        self.checkpoint = checkpoint

    def __getstate__(self) -> dict:
        return self.__dict__

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
