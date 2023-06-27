from typing import List, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class PGConfig(AlgorithmConfig):
    """Defines a configuration class from which a PG Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.pg import PGConfig
        >>> config = PGConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.pg import PGConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = PGConfig()
        >>> # Print out some default values.
        >>> print(config.lr) # doctest: +SKIP
        0.0004
        >>> # Update the config object.
        >>> config = config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "PG",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a PGConfig instance."""
        super().__init__(algo_class=algo_class or PG)

        # fmt: off
        # __sphinx_doc_begin__
        # Override some of AlgorithmConfig's default values with PG-specific values.
        self.lr_schedule = None
        self.lr = 0.0004
        self.rollout_fragment_length = "auto"
        self.train_batch_size = 200
        self._disable_preprocessor_api = True
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        **kwargs,
    ) -> "PGConfig":
        """Sets the training related configuration.

        Args:
            gamma: Float specifying the discount factor of the Markov Decision process.
            lr: The default learning rate.
            train_batch_size: Training batch size, if applicable.
            model: Arguments passed into the policy model. See models/catalog.py for a
                full list of the available model options.
            optimizer: Arguments to pass to the policy optimizer.
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        # Synchronous sampling, on-policy PG algo -> Check mismatches between
        # `rollout_fragment_length` and `train_batch_size` to avoid user confusion.
        self.validate_train_batch_size_vs_rollout_fragment_length()


@Deprecated(
    old="rllib/algorithms/pg/",
    new="rllib_contrib/pg/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class PG(Algorithm):
    """Policy Gradient (PG) Algorithm.

    Defines the distributed Algorithm class for policy gradients.
    See `pg_[tf|torch]_policy.py` for the definition of the policy losses for
    TensorFlow and PyTorch.

    Detailed documentation:
    https://docs.ray.io/en/master/rllib-algorithms.html#pg

    Only overrides the default config- and policy selectors
    (`get_default_policy_class` and `get_default_config`). Utilizes
    the default `training_step()` method of `Algorithm`.
    """

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return PGConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.pg.pg_torch_policy import PGTorchPolicy

            return PGTorchPolicy
        elif config.framework_str == "tf":
            from ray.rllib.algorithms.pg.pg_tf_policy import PGTF1Policy

            return PGTF1Policy
        else:
            from ray.rllib.algorithms.pg.pg_tf_policy import PGTF2Policy

            return PGTF2Policy
