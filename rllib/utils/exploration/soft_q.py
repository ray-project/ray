from gym.spaces import Discrete, MultiDiscrete, Space
from typing import Union, Optional

from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.stochastic_sampling import StochasticSampling
from ray.rllib.utils.framework import TensorType


@PublicAPI
class SoftQ(StochasticSampling):
    """Special case of StochasticSampling w/ Categorical and temperature param.

    Returns a stochastic sample from a Categorical parameterized by the model
    output divided by the temperature. Returns the argmax iff explore=False.
    """

    def __init__(
        self,
        action_space: Space,
        *,
        framework: Optional[str],
        temperature: float = 1.0,
        **kwargs
    ):
        """Initializes a SoftQ Exploration object.

        Args:
            action_space: The gym action space used by the environment.
            temperature: The temperature to divide model outputs by
                before creating the Categorical distribution to sample from.
            framework: One of None, "tf", "torch".
        """
        assert isinstance(action_space, (Discrete, MultiDiscrete))
        super().__init__(action_space, framework=framework, **kwargs)
        self.temperature = temperature

    @override(StochasticSampling)
    def get_exploration_action(
        self,
        action_distribution: ActionDistribution,
        timestep: Union[int, TensorType],
        explore: bool = True,
    ):
        cls = type(action_distribution)
        assert issubclass(cls, (Categorical, TorchCategorical))
        # Re-create the action distribution with the correct temperature
        # applied.
        dist = cls(action_distribution.inputs, self.model, temperature=self.temperature)
        # Delegate to super method.
        return super().get_exploration_action(
            action_distribution=dist, timestep=timestep, explore=explore
        )
