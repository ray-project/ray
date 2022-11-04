from typing import List, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated


class PGConfig(AlgorithmConfig):
    """Defines a configuration class from which a PG Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.pg import PGConfig
        >>> config = PGConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")
        >>> algo.train()

    Example:
        >>> from ray.rllib.algorithms.pg import PGConfig
        >>> from ray import air
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
        >>> tune.Tuner(
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

        # Check for mismatches between `train_batch_size` and
        # `rollout_fragment_length` (if not "auto")..
        # Note: Only check this if `train_batch_size` > 0 (DDPPO sets this
        # to -1 to auto-calculate the actual batch size later).
        if (
            self.rollout_fragment_length != "auto"
            and not self.in_evaluation
            and self.train_batch_size > 0
        ):
            min_batch_size = (
                max(self.num_rollout_workers, 1)
                * self.num_envs_per_worker
                * self.rollout_fragment_length
            )
            batch_size = min_batch_size
            while batch_size < self.train_batch_size:
                batch_size += min_batch_size
            if (
                batch_size - self.train_batch_size > 0.1 * self.train_batch_size
                or batch_size - min_batch_size - self.train_batch_size
                > (0.1 * self.train_batch_size)
            ):
                suggested_rollout_fragment_length = self.train_batch_size // (
                    self.num_envs_per_worker * (self.num_rollout_workers or 1)
                )
                raise ValueError(
                    f"Your desired `train_batch_size` ({self.train_batch_size}) or a "
                    "value 10% off of that cannot be achieved with your other "
                    f"settings (num_rollout_workers={self.num_rollout_workers}; "
                    f"num_envs_per_worker={self.num_envs_per_worker}; "
                    f"rollout_fragment_length={self.rollout_fragment_length})! "
                    "Try setting `rollout_fragment_length` to 'auto' OR "
                    f"{suggested_rollout_fragment_length}."
                )


class PG(Algorithm):
    """Policy Gradient (PG) Trainer.

    Defines the distributed Trainer class for policy gradients.
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


# Deprecated: Use ray.rllib.algorithms.pg.PGConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(PGConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.pg.default_config::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.pg.pg::PGConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
