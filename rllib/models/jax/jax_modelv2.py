import gymnasium as gym

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import ModelConfigDict
from ray.rllib.utils.deprecation import deprecation_warning
from ray.util import log_once


@PublicAPI
class JAXModelV2(ModelV2):
    """JAX version of ModelV2.

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):
        """Initializes a JAXModelV2 instance."""
        if log_once("jax_modelv2_deprecation_warning"):
            deprecation_warning(old=("ray.rllib.models.jax.jax_modelv2." "JaxModelv2"))

        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="jax",
        )
