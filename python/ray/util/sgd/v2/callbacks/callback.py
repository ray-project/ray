import abc
from typing import List, Optional, Dict


class SGDCallback(metaclass=abc.ABCMeta):
    """Abstract SGD callback class."""

    def handle_result(self, results: Optional[List[Dict]]):
        """Called every time sgd.report() is called."""
        pass

    def start_training(self):
        """Called once on training start."""
        pass

    def finish_training(self, error: bool = False):
        """Called once after training is over.

        Args:
            error (bool): If True, there was an exception during training."""
        pass
