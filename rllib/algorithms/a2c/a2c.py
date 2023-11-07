import logging
import math
from typing import Optional

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.a3c.a3c import A3CConfig, A3C
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    APPLY_GRADS_TIMER,
    COMPUTE_GRADS_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    SYNCH_WORKER_WEIGHTS_TIMER,
    SAMPLE_TIMER,
)
from ray.rllib.utils.typing import ResultDict

logger = logging.getLogger(__name__)


class A2CConfig(A3CConfig):
    """Defines a configuration class from which a new A2C can be built.

    Example:
        >>> from ray import tune
        >>> from ray.rllib.algorithms.a2c import A2CConfig
        >>> config = A2CConfig()
        >>> config = config.training(lr=0.01, grad_clip=30.0)  # doctest: +SKIP
        >>> config = config.resources(num_gpus=0)  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=2)  # doctest: +SKIP
        >>> config = config.environment("CartPole-v1")  # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray import train, tune
        >>> from ray.rllib.algorithms.a2c import A2CConfig
        >>> config = A2CConfig()
        >>> # Print out some default values.
        >>> print(config.sample_async)   # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training(lr=tune.grid_search(  # doctest: +SKIP
        ...     [0.001, 0.0001]), use_critic=False)
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1")  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "A2C",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self):
        """Initializes a A2CConfig instance."""
        super().__init__(algo_class=A2C)

        # fmt: off
        # __sphinx_doc_begin__

        # A2C specific settings:
        self.microbatch_size = None

        # Override some of A3CConfig's default values with A2C-specific values.
        self.num_rollout_workers = 2
        self.rollout_fragment_length = "auto"
        self.sample_async = False
        self.min_time_s_per_iteration = 10
        # __sphinx_doc_end__
        # fmt: on

    @override(A3CConfig)
    def training(
        self,
        *,
        microbatch_size: Optional[int] = NotProvided,
        **kwargs,
    ) -> "A2CConfig":
        """Sets the training related configuration.

        Args:
            microbatch_size: A2C supports microbatching, in which we accumulate
                gradients over batch of this size until the train batch size is reached.
                This allows training with batch sizes much larger than can fit in GPU
                memory. To enable, set this to a value less than the train batch size.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if microbatch_size is not NotProvided:
            self.microbatch_size = microbatch_size

        return self

    @override(A3CConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        # Synchronous sampling, on-policy PG algo -> Check mismatches between
        # `rollout_fragment_length` and `train_batch_size` to avoid user confusion.
        self.validate_train_batch_size_vs_rollout_fragment_length()

        if self.microbatch_size:
            if self.num_gpus > 1:
                raise AttributeError(
                    "A2C does not support multiple GPUs when micro-batching is set."
                )

            # Train batch size needs to be significantly larger than microbatch
            # size.
            if self.train_batch_size / self.microbatch_size < 3:
                logger.warning(
                    "`train_batch_size` should be considerably larger (at least 3x)"
                    " than `microbatch_size` for a microbatching setup to make "
                    "sense!"
                )
            # Rollout fragment length needs to be less than microbatch_size.
            if (
                self.rollout_fragment_length != "auto"
                and self.rollout_fragment_length > self.microbatch_size
            ):
                logger.warning(
                    "`rollout_fragment_length` should not be larger than "
                    "`microbatch_size` (try setting them to the same value)! "
                    "Otherwise, microbatches of desired size won't be achievable."
                )

    def get_rollout_fragment_length(self, worker_index: int = 0) -> int:
        if self.rollout_fragment_length == "auto":
            if self.microbatch_size:
                return self.microbatch_size
        return super().get_rollout_fragment_length(worker_index)


@Deprecated(
    old="rllib/algorithms/a2c/",
    new="rllib_contrib/a2c/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class A2C(A3C):
    @classmethod
    @override(A3C)
    def get_default_config(cls) -> AlgorithmConfig:
        return A2CConfig()

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        # Create a microbatch variable for collecting gradients on microbatches'.
        # These gradients will be accumulated on-the-fly and applied at once (once train
        # batch size has been collected) to the model.
        if self.config.microbatch_size:
            self._microbatches_grads = None
            self._microbatches_counts = self._num_microbatches = 0

    @override(A3C)
    def training_step(self) -> ResultDict:
        # Fallback to Algorithm.training_step() and A3C policies (loss_fn etc).
        # W/o microbatching: Identical to Algorithm's default implementation.
        # Only difference to a default Algorithm being the value function loss term
        # and its value computations alongside each action.
        if self.config.microbatch_size is None:
            return Algorithm.training_step(self)

        # In microbatch mode, we want to compute gradients on experience
        # microbatches, average a number of these microbatches, and then
        # apply the averaged gradient in one SGD step. This conserves GPU
        # memory, allowing for extremely large experience batches to be
        # used.
        with self._timers[SAMPLE_TIMER]:
            if self.config.count_steps_by == "agent_steps":
                train_batch = synchronous_parallel_sample(
                    worker_set=self.workers, max_agent_steps=self.config.microbatch_size
                )
            else:
                train_batch = synchronous_parallel_sample(
                    worker_set=self.workers, max_env_steps=self.config.microbatch_size
                )

        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()

        with self._timers[COMPUTE_GRADS_TIMER]:
            grad, info = self.workers.local_worker().compute_gradients(
                train_batch, single_agent=True
            )
            # New microbatch accumulation phase.
            if self._microbatches_grads is None:
                self._microbatches_grads = grad
            # Existing gradients: Accumulate new gradients on top of existing ones.
            else:
                for i, g in enumerate(grad):
                    self._microbatches_grads[i] += g
            self._microbatches_counts += train_batch.count
            self._num_microbatches += 1

        # If `train_batch_size` reached: Accumulate gradients and apply.
        num_microbatches = math.ceil(
            self.config.train_batch_size / self.config.microbatch_size
        )
        if self._num_microbatches >= num_microbatches:
            # Update counters.
            self._counters[NUM_ENV_STEPS_TRAINED] += self._microbatches_counts
            self._counters[NUM_AGENT_STEPS_TRAINED] += self._microbatches_counts

            # Apply gradients.
            apply_timer = self._timers[APPLY_GRADS_TIMER]
            with apply_timer:
                self.workers.local_worker().apply_gradients(self._microbatches_grads)
                apply_timer.push_units_processed(self._microbatches_counts)

            # Reset microbatch information.
            self._microbatches_grads = None
            self._microbatches_counts = self._num_microbatches = 0

            # Also update global vars of the local worker.
            # Create current global vars.
            global_vars = {
                "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
            }
            # Synch updated weights back to the workers
            # (only those policies that are trainable).
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.workers.sync_weights(
                    policies=self.workers.local_worker().get_policies_to_train(),
                    global_vars=global_vars,
                )

        train_results = {DEFAULT_POLICY_ID: info}

        return train_results
