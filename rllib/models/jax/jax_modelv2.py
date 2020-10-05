import gym
from typing import List

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import ModelConfigDict, TensorType


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

    #@override(ModelV2)
    #def variables(self, as_dict: bool = False) -> List[TensorType]:
    #    if as_dict:
    #        return self.state_dict()
    #    return list(self.parameters())

    #@override(ModelV2)
    #def trainable_variables(self, as_dict: bool = False) -> List[TensorType]:
    #    if as_dict:
    #        return {
    #            k: v
    #            for k, v in self.variables(as_dict=True).items()
    #            if v.requires_grad
    #        }
    #    return [v for v in self.variables() if v.requires_grad]
