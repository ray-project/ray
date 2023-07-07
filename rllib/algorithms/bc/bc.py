from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.utils.annotations import override


class BCConfig(MARWILConfig):
    """Defines a configuration class from which a new BC Algorithm can be built

    Example:
        >>> from ray.rllib.algorithms.bc import BCConfig
        >>> # Run this from the ray directory root.
        >>> config = BCConfig().training(lr=0.00001, gamma=0.99)
        >>> config = config.offline_data(  # doctest: +SKIP
        ...     input_="./rllib/tests/data/cartpole/large.json")
        >>> print(config.to_dict())  # doctest:+SKIP
        >>> # Build an Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.bc import BCConfig
        >>> from ray import tune
        >>> config = BCConfig()
        >>> # Print out some default values.
        >>> print(config.beta)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config.training(  # doctest:+SKIP
        ...     lr=tune.grid_search([0.001, 0.0001]), beta=0.75
        ... )
        >>> # Set the config object's data path.
        >>> # Run this from the ray directory root.
        >>> config.offline_data(  # doctest:+SKIP
        ...     input_="./rllib/tests/data/cartpole/large.json"
        ... )
        >>> # Set the config object's env, used for evaluation.
        >>> config.environment(env="CartPole-v1")  # doctest:+SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(   # doctest:+SKIP
        ...     "BC",
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or BC)

        # fmt: off
        # __sphinx_doc_begin__
        # No need to calculate advantages (or do anything else with the rewards).
        self.beta = 0.0
        # Advantages (calculated during postprocessing)
        # not important for behavioral cloning.
        self.postprocess_inputs = False
        # __sphinx_doc_end__
        # fmt: on

    @override(MARWILConfig)
    def validate(self) -> None:
        super().validate()

        if self.beta != 0.0:
            raise ValueError("For behavioral cloning, `beta` parameter must be 0.0!")


class BC(MARWIL):
    """Behavioral Cloning (derived from MARWIL).

    Simply uses MARWIL with beta force-set to 0.0.
    """

    @classmethod
    @override(MARWIL)
    def get_default_config(cls) -> AlgorithmConfig:
        return BCConfig()
