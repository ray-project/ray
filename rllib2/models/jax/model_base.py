import abc
import tree
import jax
from jax import numpy as jnp
import equinox as eqx

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
        

class JaxRecurrentModel(
    RecurrentModel,
    eqx.Module, 
    ModelIO
):

    def __init__(self, config: ModelConfig, key: jax.random.PRNGKeyArray) -> None:
        super(RecurrentModel).__init__(name=config.name)
        super(ModelIO).__init__()
        self._config = config


    
    def _initial_state(self) -> NestedDict[jnp.ndarray]:
        return tree.map_structure(
            lambda spec: jnp.zeros(spec.shape, dtype=spec.dtype),
            self.initial_state_spec
        )
    
class JaxModel(
    Model,
    eqx.Module, 
    ModelIO
):
    
    def __init__(self, config: ModelConfig) -> None:
        super(Model).__init__(name=config.name)
        super(ModelIO).__init__()
        self._config = config
