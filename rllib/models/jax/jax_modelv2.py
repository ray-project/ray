import gym
import time
import tree
from typing import Dict, List, Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.framework import try_import_jax
from ray.rllib.utils.typing import ModelConfigDict, TensorType

jax, flax = try_import_jax()
fd = None
if flax:
    from flax.core.frozen_dict import FrozenDict as fd


@PublicAPI
class JAXModelV2(ModelV2):
    """JAX version of ModelV2.



    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):
        """Initializes a JAXModelV2 instance."""

        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="jax")

        self.prng_key = jax.random.PRNGKey(int(time.time()))

    @PublicAPI
    @override(ModelV2)
    def variables(self, as_dict: bool = False
                  ) -> Union[List[TensorType], Dict[str, TensorType]]:
        params = {
            k: v
            for k, v in self.__dict__.items()
            if isinstance(v, fd) and "params" in v
        }
        if as_dict:
            return params
        return tree.flatten(params)

    @PublicAPI
    @override(ModelV2)
    def trainable_variables(
            self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        return self.variables(as_dict=as_dict)

    @PublicAPI
    @override(ModelV2)
    def value_function(self) -> TensorType:
        raise ValueError("JAXModelV2 does not have a separate "
                         "`value_function()` call!")
