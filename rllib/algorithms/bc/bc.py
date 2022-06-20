from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.typing import AlgorithmConfigDict


class BCConfig(MARWILConfig):
    """Defines a configuration class from which a new BC Trainer can be built

    Example:
        >>> from ray.rllib.algorithms.bc import BCConfig
        >>> # Run this from the ray directory root.
        >>> config = BCConfig().training(lr=0.00001, gamma=0.99)\
        ...             .offline_data(input_="./rllib/tests/data/cartpole/large.json")
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build()
        >>> trainer.train()

    Example:
        >>> from ray.rllib.algorithms.bc import BCConfig
        >>> from ray import tune
        >>> config = BCConfig()
        >>> # Print out some default values.
        >>> print(config.beta)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]), beta=0.75)
        >>> # Set the config object's data path.
        >>> # Run this from the ray directory root.
        >>> config.offline_data(input_="./rllib/tests/data/cartpole/large.json")
        >>> # Set the config object's env, used for evaluation.
        >>> config.environment(env="CartPole-v0")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "BC",
        ...     config=config.to_dict(),
        ... )
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
        # No reward estimation.
        self.off_policy_estimation_methods = {}
        # __sphinx_doc_end__
        # fmt: on


class BC(MARWIL):
    """Behavioral Cloning (derived from MARWIL).

    Simply uses MARWIL with beta force-set to 0.0.
    """

    @classmethod
    @override(MARWIL)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return BCConfig().to_dict()

    @override(MARWIL)
    def validate_config(self, config: AlgorithmConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["beta"] != 0.0:
            raise ValueError("For behavioral cloning, `beta` parameter must be 0.0!")


# Deprecated: Use ray.rllib.algorithms.bc.BCConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(BCConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.marwil.bc::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.bc.bc::BCConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


BC_DEFAULT_CONFIG = _deprecated_default_config()
