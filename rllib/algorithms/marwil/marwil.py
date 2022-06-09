from typing import Optional, Type

from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.offline.estimators import ImportanceSampling, WeightedImportanceSampling
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, DEPRECATED_VALUE
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    WORKER_UPDATE_TIMER,
)
from ray.rllib.utils.typing import (
    ResultDict,
    TrainerConfigDict,
)
from ray.rllib.utils.replay_buffers.utils import sample_min_n_steps_from_buffer


class MARWILConfig(TrainerConfig):
    """Defines a configuration class from which a MARWIL Trainer can be built.


    Example:
        >>> from ray.rllib.algorithms.marwil import MARWILConfig
        >>> # Run this from the ray directory root.
        >>> config = MARWILConfig().training(beta=1.0, lr=0.00001, gamma=0.99)\
        ...             .offline_data(input_=["./rllib/tests/data/cartpole/large.json"])
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build()
        >>> trainer.train()

    Example:
        >>> from ray.rllib.algorithms.marwil import MARWILConfig
        >>> from ray import tune
        >>> config = MARWILConfig()
        >>> # Print out some default values.
        >>> print(config.beta)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]), beta=0.75)
        >>> # Set the config object's data path.
        >>> # Run this from the ray directory root.
        >>> config.offline_data(input_=["./rllib/tests/data/cartpole/large.json"])
        >>> # Set the config object's env, used for evaluation.
        >>> config.environment(env="CartPole-v0")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "MARWIL",
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self, trainer_class=None):
        """Initializes a MARWILConfig instance."""
        super().__init__(trainer_class=trainer_class or MARWIL)

        # fmt: off
        # __sphinx_doc_begin__
        # MARWIL specific settings:
        self.beta = 1.0
        self.bc_logstd_coeff = 0.0
        self.moving_average_sqd_adv_norm_update_rate = 1e-8
        self.moving_average_sqd_adv_norm_start = 100.0
        self.replay_buffer_config = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            # Size of the replay buffer in (single and independent) timesteps.
            # The buffer gets filled by reading from the input files line-by-line
            # and adding all timesteps on one line at once. We then sample
            # uniformly from the buffer (`train_batch_size` samples) for
            # each training step.
            "capacity": 10000,
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization
            "prioritized_replay": DEPRECATED_VALUE,
            # Number of steps to read before learning starts.
            "learning_starts": 0,
            "replay_sequence_length": 1
        }
        self.use_gae = True
        self.vf_coeff = 1.0
        self.grad_clip = None

        # Override some of TrainerConfig's default values with MARWIL-specific values.

        # You should override input_ to point to an offline dataset
        # (see trainer.py and trainer_config.py).
        # The dataset may have an arbitrary number of timesteps
        # (and even episodes) per line.
        # However, each line must only contain consecutive timesteps in
        # order for MARWIL to be able to calculate accumulated
        # discounted returns. It is ok, though, to have multiple episodes in
        # the same line.
        self.input_ = "sampler"
        # Use importance sampling estimators for reward.
        self.off_policy_estimation_methods = {
            "is": {"type": ImportanceSampling},
            "wis": {"type": WeightedImportanceSampling},
        }
        self.postprocess_inputs = True
        self.lr = 1e-4
        self.train_batch_size = 2000
        self.num_workers = 0
        # __sphinx_doc_end__
        # fmt: on

    @override(TrainerConfig)
    def training(
        self,
        *,
        beta: Optional[float] = None,
        bc_logstd_coeff: Optional[float] = None,
        moving_average_sqd_adv_norm_update_rate: Optional[float] = None,
        moving_average_sqd_adv_norm_start: Optional[float] = None,
        replay_buffer_config: Optional[dict] = None,
        use_gae: Optional[bool] = True,
        vf_coeff: Optional[float] = None,
        grad_clip: Optional[float] = None,
        **kwargs,
    ) -> "MARWILConfig":
        """Sets the training related configuration.

        Args:
            beta: Scaling  of advantages in exponential terms. When beta is 0.0,
                MARWIL is reduced to behavior cloning (imitation learning);
                see bc.py algorithm in this same directory.
            bc_logstd_coeff: A coefficient to encourage higher action distribution
                entropy for exploration.
            moving_average_sqd_adv_norm_start: Starting value for the
                squared moving average advantage norm (c^2).
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "learning_starts": 1000,
                "capacity": 50000,
                "replay_batch_size": 32,
                "replay_sequence_length": 1,
                }
                - OR -
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 50000,
                "prioritized_replay_alpha": 0.6,
                "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
                "replay_sequence_length": 1,
                }
                - Where -
                prioritized_replay_alpha: Alpha parameter controls the degree of
                prioritization in the buffer. In other words, when a buffer sample has
                a higher temporal-difference error, with how much more probability
                should it drawn to use to update the parametrized Q-network. 0.0
                corresponds to uniform probability. Setting much above 1.0 may quickly
                result as the sampling distribution could become heavily “pointy” with
                low entropy.
                prioritized_replay_beta: Beta parameter controls the degree of
                importance sampling which suppresses the influence of gradient updates
                from samples that have higher probability of being sampled via alpha
                parameter and the temporal-difference error.
                prioritized_replay_eps: Epsilon parameter sets the baseline probability
                for sampling so that when the temporal-difference error of a sample is
                zero, there is still a chance of drawing the sample.
            use_gae: If true, use the Generalized Advantage Estimator (GAE)
                with a value function, see https://arxiv.org/pdf/1506.02438.pdf in
                case an input line ends with a non-terminal timestep.
            vf_coeff: Balancing value estimation loss and policy optimization loss.
                moving_average_sqd_adv_norm_update_rate: Update rate for the
                squared moving average advantage norm (c^2).
            grad_clip: If specified, clip the global norm of gradients by this amount.

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)
        if beta is not None:
            self.beta = beta
        if bc_logstd_coeff is not None:
            self.bc_logstd_coeff = bc_logstd_coeff
        if moving_average_sqd_adv_norm_update_rate is not None:
            self.moving_average_sqd_adv_norm_update_rate = (
                moving_average_sqd_adv_norm_update_rate
            )
        if moving_average_sqd_adv_norm_start is not None:
            self.moving_average_sqd_adv_norm_start = moving_average_sqd_adv_norm_start
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if use_gae is not None:
            self.use_gae = use_gae
        if vf_coeff is not None:
            self.vf_coeff = vf_coeff
        if grad_clip is not None:
            self.grad_clip = grad_clip
        return self


class MARWIL(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return MARWILConfig().to_dict()

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        # TODO: Move this to super()?
        validate_buffer_config(config)
        if config["beta"] < 0.0 or config["beta"] > 1.0:
            raise ValueError("`beta` must be within 0.0 and 1.0!")

        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for MARWIL!")

        if config["postprocess_inputs"] is False and config["beta"] > 0.0:
            raise ValueError(
                "`postprocess_inputs` must be True for MARWIL (to "
                "calculate accum., discounted returns)!"
            )

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.marwil.marwil_torch_policy import (
                MARWILTorchPolicy,
            )

            return MARWILTorchPolicy
        elif config["framework"] == "tf":
            from ray.rllib.algorithms.marwil.marwil_tf_policy import (
                MARWILTF1Policy,
            )

            return MARWILTF1Policy
        else:
            from ray.rllib.algorithms.marwil.marwil_tf_policy import MARWILTF2Policy

            return MARWILTF2Policy

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        # Collect SampleBatches from sample workers.
        batch = synchronous_parallel_sample(worker_set=self.workers)
        batch = batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()
        # Add batch to replay buffer.
        self.local_replay_buffer.add(batch)

        # Pull batch from replay buffer and train on it.
        train_batch = sample_min_n_steps_from_buffer(
            self.local_replay_buffer,
            self.config["train_batch_size"],
            count_by_agent_steps=self._by_agent_steps,
        )
        # Train.
        if self.config["simple_optimizer"]:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # TODO: Move training steps counter update outside of `train_one_step()` method.
        # # Update train step counters.
        # self._counters[NUM_ENV_STEPS_TRAINED] += train_batch.env_steps()
        # self._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

        global_vars = {
            "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
        }

        # Update weights - after learning on the local worker - on all remote
        # workers.
        if self.workers.remote_workers():
            with self._timers[WORKER_UPDATE_TIMER]:
                self.workers.sync_weights(global_vars=global_vars)

        # Update global vars on local worker as well.
        self.workers.local_worker().set_global_vars(global_vars)

        return train_results


# Deprecated: Use ray.rllib.algorithms.marwil.MARWILConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(MARWILConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.marwil.marwil::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.marwil.marwil::MARWILConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
