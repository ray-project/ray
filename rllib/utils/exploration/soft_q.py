from gym.spaces import Discrete

from ray.rllib.utils.exploration.stochastic_sampling import StochasticSampling


class SoftQ(StochasticSampling):
    """Special case of StochasticSampling w/ Categorical and temperature param.

    Returns a stochastic sample from a Categorical parameterized by the model
    output divided by the temperature. Returns the argmax iff explore=False.
    """

    def __init__(self,
                 action_space,
                 *,
                 temperature=1.0,
                 framework="tf",
                 **kwargs):
        """Initializes a SoftQ Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            temperature (Schedule): The temperature to divide model outputs by
                before creating the Categorical distribution to sample from.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        assert isinstance(action_space, Discrete)
        super().__init__(
            action_space,
            static_params=dict(temperature=temperature),
            framework=framework,
            **kwargs)
