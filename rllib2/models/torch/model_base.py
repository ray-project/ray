import abc

import torch
import torch.nn as nn
import tree

from ..configs import ModelConfig
from ..modular import Model, RecurrentModel
from ..types import NestedDict


# TODO: implement this class
class ModelIO(abc.ABC):
    def __init__(self, config: ModelConfig) -> None:
        self._config = config

    @property
    def config(self) -> ModelConfig:
        return self._config

    @abc.absrtactmethod
    def save(self, path: str) -> None:
        raise NotImplementedError

    @abc.absrtactmethod
    def load(self, path: str) -> RecurrentModel:
        raise NotImplementedError


class TorchRecurrentModel(RecurrentModel, nn.Module, ModelIO):
    def __init__(self, config: ModelConfig) -> None:
        super(RecurrentModel).__init__(name=config.name)
        super(nn.Module).__init__()
        super(ModelIO).__init__()
        self._config = config

    def _initial_state(self) -> NestedDict[torch.Tensor]:
        return tree.map_structure(
            lambda spec: torch.zeros(spec.shape, dtype=spec.dtype),
            self.initial_state_spec,
        )


class TorchModel(Model, nn.Module, ModelIO):
    def __init__(self, config: ModelConfig) -> None:
        super(Model).__init__(name=config.name)
        super(nn.Module).__init__()
        super(ModelIO).__init__()
        self._config = config
