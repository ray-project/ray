from typing import Optional, Type

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.simple_q.simple_q import SimpleQ, SimpleQConfig
from ray.rllib.algorithms.qmix.qmix_policy import QMixTorchPolicy
from ray.rllib.utils.replay_buffers.utils import update_priorities_in_replay_buffer
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.replay_buffers.utils import sample_min_n_steps_from_buffer
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.deprecation import deprecation_warning


class QMixConfig(SimpleQConfig):
    """Defines a configuration class from which QMix can be built.

    Example:
        >>> from ray.rllib.examples.env.two_step_game import TwoStepGame
        >>> from ray.rllib.algorithms.qmix import QMixConfig
        >>> config = QMixConfig()  # doctest: +SKIP
        >>> config = config.training(gamma=0.9, lr=0.01, kl_coeff=0.3)  # doctest: +SKIP
        >>> config = config.resources(num_gpus=0)  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=4)  # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build an Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env=TwoStepGame)  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.examples.env.two_step_game import TwoStepGame
        >>> from ray.rllib.algorithms.qmix import QMixConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = QMixConfig()
        >>> # Print out some default values.
        >>> print(config.optim_alpha)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config.training(  # doctest: +SKIP
        ...     lr=tune.grid_search([0.001, 0.0001]), optim_alpha=0.97
        ... )
        >>> # Set the config object's env.
        >>> config.environment(env=TwoStepGame)  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "QMix",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self):
        """Initializes a PPOConfig instance."""
        super().__init__(algo_class=QMix)

        # fmt: off
        # __sphinx_doc_begin__
        # QMix specific settings:
        self.mixer = "qmix"
        self.mixing_embed_dim = 32
        self.double_q = True
        self.optim_alpha = 0.99
        self.optim_eps = 0.00001
        self.grad_clip = 10

        # QMix-torch overrides the TorchPolicy's learn_on_batch w/o specifying a
        # alternative `learn_on_loaded_batch` alternative for the GPU.
        # TODO: This hack will be resolved once we move all algorithms to the new
        #  RLModule/RLTrainer APIs.
        self.simple_optimizer = True

        # Override some of AlgorithmConfig's default values with QMix-specific values.
        # .training()
        self.lr = 0.0005
        self.train_batch_size = 32
        self.target_network_update_freq = 500
        self.num_steps_sampled_before_learning_starts = 1000
        self.replay_buffer_config = {
            "type": "ReplayBuffer",
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            # Size of the replay buffer in batches (not timesteps!).
            "capacity": 1000,
            # Choosing `fragments` here makes it so that the buffer stores entire
            # batches, instead of sequences, episodes or timesteps.
            "storage_unit": "fragments",
            # Whether to compute priorities on workers.
            "worker_side_prioritization": False,
        }
        self.model = {
            "lstm_cell_size": 64,
            "max_seq_len": 999999,
        }

        # .framework()
        self.framework_str = "torch"

        # .rollouts()
        self.rollout_fragment_length = 4
        self.batch_mode = "complete_episodes"

        # .reporting()
        self.min_time_s_per_iteration = 1
        self.min_sample_timesteps_per_iteration = 1000

        # .exploration()
        self.exploration_config = {
            # The Exploration class to use.
            "type": "EpsilonGreedy",
            # Config for the Exploration class' constructor:
            "initial_epsilon": 1.0,
            "final_epsilon": 0.01,
            # Timesteps over which to anneal epsilon.
            "epsilon_timesteps": 40000,

            # For soft_q, use:
            # "exploration_config" = {
            #   "type": "SoftQ"
            #   "temperature": [float, e.g. 1.0]
            # }
        }

        # .evaluation()
        # Evaluate with epsilon=0 every `evaluation_interval` training iterations.
        # The evaluation stats will be reported under the "evaluation" metric key.
        self.evaluation(
            evaluation_config=AlgorithmConfig.overrides(explore=False)
        )
        # __sphinx_doc_end__
        # fmt: on

        self.worker_side_prioritization = DEPRECATED_VALUE

    @override(SimpleQConfig)
    def training(
        self,
        *,
        mixer: Optional[str] = NotProvided,
        mixing_embed_dim: Optional[int] = NotProvided,
        double_q: Optional[bool] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        replay_buffer_config: Optional[dict] = NotProvided,
        optim_alpha: Optional[float] = NotProvided,
        optim_eps: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        # Deprecated args.
        grad_norm_clipping=DEPRECATED_VALUE,
        **kwargs,
    ) -> "QMixConfig":
        """Sets the training related configuration.

        Args:
            mixer: Mixing network. Either "qmix", "vdn", or None.
            mixing_embed_dim: Size of the mixing network embedding.
            double_q: Whether to use Double_Q learning.
            target_network_update_freq: Update the target network every
                `target_network_update_freq` sample steps.
            replay_buffer_config:
            optim_alpha: RMSProp alpha.
            optim_eps: RMSProp epsilon.
            grad_clip: If not None, clip gradients during optimization at
                this value.
            grad_norm_clipping: Depcrecated in favor of grad_clip

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if grad_norm_clipping != DEPRECATED_VALUE:
            deprecation_warning(
                old="grad_norm_clipping",
                new="grad_clip",
                help="Parameter `grad_norm_clipping` has been "
                "deprecated in favor of grad_clip in QMix. "
                "This is now the same parameter as in other "
                "algorithms. `grad_clip` will be overwritten by "
                "`grad_norm_clipping={}`".format(grad_norm_clipping),
                error=True,
            )
            grad_clip = grad_norm_clipping

        if mixer is not NotProvided:
            self.mixer = mixer
        if mixing_embed_dim is not NotProvided:
            self.mixing_embed_dim = mixing_embed_dim
        if double_q is not NotProvided:
            self.double_q = double_q
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config = replay_buffer_config
        if optim_alpha is not NotProvided:
            self.optim_alpha = optim_alpha
        if optim_eps is not NotProvided:
            self.optim_eps = optim_eps
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip

        return self

    @override(SimpleQConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.framework_str != "torch":
            raise ValueError(
                "Only `config.framework('torch')` supported so far for QMix!"
            )


class QMix(SimpleQ):
    @classmethod
    @override(SimpleQ)
    def get_default_config(cls) -> AlgorithmConfig:
        return QMixConfig()

    @classmethod
    @override(SimpleQ)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        return QMixTorchPolicy

    @override(SimpleQ)
    def training_step(self) -> ResultDict:
        """QMIX training iteration function.

        - Sample n MultiAgentBatches from n workers synchronously.
        - Store new samples in the replay buffer.
        - Sample one training MultiAgentBatch from the replay buffer.
        - Learn on the training batch.
        - Update the target network every `target_network_update_freq` sample steps.
        - Return all collected training metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        # Sample n batches from n workers.
        new_sample_batches = synchronous_parallel_sample(
            worker_set=self.workers, concat=False
        )

        for batch in new_sample_batches:
            # Update counters.
            self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()
            self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
            # Store new samples in the replay buffer.
            self.local_replay_buffer.add(batch)

        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_SAMPLED
        ]

        train_results = {}

        if cur_ts > self.config.num_steps_sampled_before_learning_starts:
            # Sample n batches from replay buffer until the total number of timesteps
            # reaches `train_batch_size`.
            train_batch = sample_min_n_steps_from_buffer(
                replay_buffer=self.local_replay_buffer,
                min_steps=self.config.train_batch_size,
                count_by_agent_steps=self.config.count_steps_by == "agent_steps",
            )

            # Learn on the training batch.
            # Use simple optimizer (only for multi-agent or tf-eager; all other
            # cases should use the multi-GPU optimizer, even if only using 1 GPU)
            if self.config.get("simple_optimizer") is True:
                train_results = train_one_step(self, train_batch)
            else:
                train_results = multi_gpu_train_one_step(self, train_batch)

            # Update target network every `target_network_update_freq` sample steps.
            last_update = self._counters[LAST_TARGET_UPDATE_TS]
            if cur_ts - last_update >= self.config.target_network_update_freq:
                to_update = self.workers.local_worker().get_policies_to_train()
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
                self._counters[NUM_TARGET_UPDATES] += 1
                self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

            update_priorities_in_replay_buffer(
                self.local_replay_buffer, self.config, train_batch, train_results
            )

            # Update weights and global_vars - after learning on the local worker -
            # on all remote workers.
            global_vars = {
                "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
            }
            # Update remote workers' weights and global vars after learning on local
            # worker.
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.workers.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results


# Deprecated: Use ray.rllib.algorithms.qmix.qmix.QMixConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(QMixConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.qmix.qmix.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.qmix.qmix.QMixConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
