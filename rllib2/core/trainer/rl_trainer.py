import abc

from rllib2.core import RLModule

class RLTrainer:

    def __init__(self) -> None:
        self._model: RLModule = ...

    @abc.abstractmethod
    def update(self, samples: BatchType, **kwargs) -> Any:
        pass
