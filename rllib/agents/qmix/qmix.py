from typing import Optional, Type

from ray.rllib.agents.dqn.simple_q import SimpleQConfig, SimpleQTrainer
from ray.rllib.agents.qmix.qmix_policy import QMixTorchPolicy
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, Deprecated
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.replay_buffers.utils import sample_min_n_steps_from_buffer
from ray.rllib.utils.typing import ResultDict, TrainerConfigDict


class QMixConfig(SimpleQConfig):
    """Defines a configuration class from which a QMixTrainer can be built.

    Example:
        >>> from ray.rllib.examples.env.two_step_game import TwoStepGame
        >>> from ray.rllib.agents.qmix import QMixConfig
        >>> config = QMixConfig().training(gamma=0.9, lr=0.01, kl_coeff=0.3)\
        ...             .resources(num_gpus=0)\
        ...             .rollouts(num_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env=TwoStepGame)
        >>> trainer.train()

    Example:
        >>> from ray.rllib.examples.env.two_step_game import TwoStepGame
        >>> from ray.rllib.agents.qmix import QMixConfig
        >>> from ray import tune
        >>> config = QMixConfig()
        >>> # Print out some default values.
        >>> print(config.optim_alpha)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]), optim_alpha=0.97)
        >>> # Set the config object's env.
        >>> config.environment(env=TwoStepGame)
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "QMix",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self):
        """Initializes a PPOConfig instance."""
        super().__init__(trainer_class=QMixTrainer)

        # fmt: off
        # __sphinx_doc_begin__
        # QMix specific settings:
        self.mixer = "qmix"
        self.mixing_embed_dim = 32
        self.double_q = True
        self.target_network_update_freq = 500
        self.replay_buffer_config = {
            # Use the new ReplayBuffer API here
            "_enable_replay_buffer_api": True,
            "type": "SimpleReplayBuffer",
            # Size of the replay buffer in batches (not timesteps!).
            "capacity": 1000,
            "learning_starts": 1000,
        }
        self.optim_alpha = 0.99
        self.optim_eps = 0.00001
        self.grad_norm_clipping = 10
        self.worker_side_prioritization = False

        # Override some of TrainerConfig's default values with QMix-specific values.
        self.num_workers = 0
        self.min_time_s_per_reporting = 1
        self.model = {
            "lstm_cell_size": 64,
            "max_seq_len": 999999,
        }
        self.framework_str = "torch"
        self.lr = 0.0005
        self.rollout_fragment_length = 4
        self.train_batch_size = 32
        self.batch_mode = "complete_episodes"
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

        # Evaluate with epsilon=0 every `evaluation_interval` training iterations.
        # The evaluation stats will be reported under the "evaluation" metric key.
        # Note that evaluation is currently not parallelized, and that for Ape-X
        # metrics are already only reported for the lowest epsilon workers.
        self.evaluation_interval = None
        self.evaluation_duration = 10
        self.evaluation_config = {
            "explore": False,
        }
        self.min_sample_timesteps_per_reporting = 1000
        # __sphinx_doc_end__
        # fmt: on

        # Deprecated keys:
        self.learning_starts = DEPRECATED_VALUE
        self.buffer_size = DEPRECATED_VALUE

    @override(SimpleQConfig)
    def training(
        self,
        *,
        mixer: Optional[str] = None,
        mixing_embed_dim: Optional[int] = None,
        double_q: Optional[bool] = None,
        target_network_update_freq: Optional[int] = None,
        replay_buffer_config: Optional[dict] = None,
        optim_alpha: Optional[float] = None,
        optim_eps: Optional[float] = None,
        grad_norm_clipping: Optional[float] = None,
        worker_side_prioritization: Optional[bool] = None,
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
            grad_norm_clipping: If not None, clip gradients during optimization at
                this value.
            worker_side_prioritization: Whether to compute priorities for the replay
                buffer on worker side.

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if mixer is not None:
            self.mixer = mixer
        if mixing_embed_dim is not None:
            self.mixing_embed_dim = mixing_embed_dim
        if double_q is not None:
            self.double_q = double_q
        if target_network_update_freq is not None:
            self.target_network_update_freq = target_network_update_freq
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if optim_alpha is not None:
            self.optim_alpha = optim_alpha
        if optim_eps is not None:
            self.optim_eps = optim_eps
        if grad_norm_clipping is not None:
            self.grad_norm_clipping = grad_norm_clipping
        if worker_side_prioritization is not None:
            self.worker_side_prioritization = worker_side_prioritization

        return self


class QMixTrainer(SimpleQTrainer):
    @classmethod
    @override(SimpleQTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return QMixConfig().to_dict()

    @override(SimpleQTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["framework"] != "torch":
            raise ValueError("Only `framework=torch` supported so far for QMixTrainer!")

    @override(SimpleQTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        return QMixTorchPolicy

    @override(SimpleQTrainer)
    def training_iteration(self) -> ResultDict:
        """QMIX training iteration function.

        - Sample n MultiAgentBatches from n workers synchronously.
        - Store new samples in the replay buffer.
        - Sample one training MultiAgentBatch from the replay buffer.
        - Learn on the training batch.
        - Update the target network every `target_network_update_freq` steps.
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

        # Sample n batches from replay buffer until the total number of timesteps
        # reaches `train_batch_size`.
        train_batch = sample_min_n_steps_from_buffer(
            replay_buffer=self.local_replay_buffer,
            min_steps=self.config["train_batch_size"],
            count_by_agent_steps=self._by_agent_steps,
        )
        if train_batch is None:
            return {}

        # Learn on the training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # TODO: Move training steps counter update outside of `train_one_step()` method.
        # # Update train step counters.
        # self._counters[NUM_ENV_STEPS_TRAINED] += train_batch.env_steps()
        # self._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

        # Update target network every `target_network_update_freq` steps.
        cur_ts = self._counters[NUM_ENV_STEPS_SAMPLED]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update >= self.config["target_network_update_freq"]:
            to_update = self.workers.local_worker().get_policies_to_train()
            self.workers.local_worker().foreach_policy_to_train(
                lambda p, pid: pid in to_update and p.update_target()
            )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        # Update weights and global_vars - after learning on the local worker - on all
        # remote workers.
        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }
        # Update remote workers' weights and global vars after learning on local worker.
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            self.workers.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results


# Deprecated: Use ray.rllib.agents.qmix.qmix.QMixConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(QMixConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.qmix.qmix.DEFAULT_CONFIG",
        new="ray.rllib.agents.qmix.qmix.QMixConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
