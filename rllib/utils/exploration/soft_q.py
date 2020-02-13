from ray.rllib.utils.exploration.stochastic_sampling import StochasticSampling


class SoftQ(StochasticSampling):
    """
    """

    def __init__(self, action_space, temperature=1.0, framework="tf",
                 **kwargs):
        """

        Args:
            action_space (Space): The gym action space used by the environment.
            temperature (Schedule):
            framework (Optional[str]): One of None, "tf", "torch".
            time_dependent_params (dict):
        """
        super().__init__(
            action_space=action_space,
            static_params=dict(temperature=temperature),
            framework=framework,
            **kwargs)
