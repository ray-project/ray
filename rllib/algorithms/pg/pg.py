from typing import Type

from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.execution.parallel_requests import synchronous_parallel_sample
from ray.rllib.execution.train_ops import train_one_step, multi_gpu_train_one_step
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_SAMPLED, NUM_ENV_STEPS_SAMPLED, \
    WORKER_UPDATE_TIMER
from ray.rllib.utils.typing import TrainerConfigDict, ResultDict


class PGConfig(TrainerConfig):
    """Defines a configuration class from which a PG Trainer can be built.

    Example:
        >>> from ray.rllib.algorithms.pg import PGConfig
        >>> config = PGConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
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
        super().__init__(trainer_class=PG)

        # fmt: off
        # __sphinx_doc_begin__
        # Override some of TrainerConfig's default values with PG-specific values.
        self.num_workers = 0
        self.lr = 0.0004
        self._disable_preprocessor_api = True
        # __sphinx_doc_end__
        # fmt: on


class PG(Trainer):
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
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return PGConfig().to_dict()

    @override(Trainer)
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

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        """Single Iteration of Vanilla Policy Gradient Algorithm.

        - Collect on-policy samples (SampleBatches) in parallel using the
          Trainer's RolloutWorkers (@ray.remote).
        - Concatenate collected SampleBatches into one train batch.
        - Note that we may have more than one policy in the multi-agent case:
          Call the different policies' `learn_on_batch` (simple optimizer) OR
          `load_batch_into_buffer` + `learn_on_loaded_batch` (multi-GPU
          optimizer) methods to calculate loss and update the model(s).
        - Return all collected metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        # Collect SampleBatches from sample workers until we have a full batch.
        if self._by_agent_steps:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_agent_steps=self.config["train_batch_size"]
            )
        else:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_env_steps=self.config["train_batch_size"]
            )
        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU).
        # TODO: (sven) rename MultiGPUOptimizer into something more
        #  meaningful.
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # Update weights and global_vars - after learning on the local worker - on all
        # remote workers.
        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }
        with self._timers[WORKER_UPDATE_TIMER]:
            self.workers.sync_weights(global_vars=global_vars)

        return train_results


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
