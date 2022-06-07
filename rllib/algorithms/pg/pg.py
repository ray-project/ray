from typing import Type

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.typing import AlgorithmConfigDict


class PGConfig(AlgorithmConfig):
    """Defines a configuration class from which a PG Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.pg import PGConfig
        >>> config = PGConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> from ray.rllib.algorithms.pg import PGConfig
        >>> from ray import tune
        >>> config = PGConfig()
        >>> # Print out some default values.
        >>> print(config.lr) # doctest: +SKIP
        0.0004
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "PG",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self):
        """Initializes a PGConfig instance."""
        super().__init__(algo_class=PG)

        # fmt: off
        # __sphinx_doc_begin__
        # Override some of AlgorithmConfig's default values with PG-specific values.
        self.num_workers = 0
        self.lr = 0.0004
        self._disable_preprocessor_api = True
        # __sphinx_doc_end__
        # fmt: on


class PG(Algorithm):
    """Policy Gradient (PG) Trainer.

    Defines the distributed Trainer class for policy gradients.
    See `pg_[tf|torch]_policy.py` for the definition of the policy losses for
    TensorFlow and PyTorch.

    Detailed documentation:
    https://docs.ray.io/en/master/rllib-algorithms.html#pg

    Only overrides the default config- and policy selectors
    (`get_default_policy` and `get_default_config`). Utilizes
    the default `execution_plan()` of `Trainer`.
    """

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return PGConfig().to_dict()

    @override(Algorithm)
    def get_default_policy_class(self, config) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.pg.pg_torch_policy import PGTorchPolicy

            return PGTorchPolicy
        elif config["framework"] == "tf":
            from ray.rllib.algorithms.pg.pg_tf_policy import PGStaticGraphTFPolicy

            return PGStaticGraphTFPolicy
        else:
            from ray.rllib.algorithms.pg.pg_tf_policy import PGEagerTFPolicy

            return PGEagerTFPolicy


# Deprecated: Use ray.rllib.algorithms.pg.PGConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(PGConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.pg.default_config::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.pg.pg::PGConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
