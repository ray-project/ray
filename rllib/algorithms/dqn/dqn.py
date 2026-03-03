"""
Deep Q-Networks (DQN, Rainbow, Parametric DQN)
==============================================

This file defines the distributed Algorithm class for the Deep Q-Networks
algorithm. See `dqn_[tf|torch]_policy.py` for the definition of the policies.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#deep-q-networks-dqn-rainbow-parametric-dqn
"""  # noqa: E501

import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import numpy as np
from typing_extensions import Self

from ray._common.deprecation import DEPRECATED_VALUE
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.core.learner import Learner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils import deep_update
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    LAST_TARGET_UPDATE_TS,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_TARGET_UPDATES,
    REPLAY_BUFFER_ADD_DATA_TIMER,
    REPLAY_BUFFER_RESULTS,
    REPLAY_BUFFER_SAMPLE_TIMER,
    REPLAY_BUFFER_UPDATE_PRIOS_TIMER,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TD_ERROR_KEY,
    TIMERS,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.replay_buffers.utils import (
    sample_min_n_steps_from_buffer,
    update_priorities_in_episode_replay_buffer,
    update_priorities_in_replay_buffer,
    validate_buffer_config,
)
from ray.rllib.utils.typing import (
    LearningRateOrSchedule,
    ResultDict,
    RLModuleSpecType,
    SampleBatchType,
)

logger = logging.getLogger(__name__)


class DQNConfig(AlgorithmConfig):
    r"""Defines a configuration class from which a DQN Algorithm can be built.

    .. testcode::

        from ray.rllib.algorithms.dqn.dqn import DQNConfig

        config = (
            DQNConfig()
            .environment("CartPole-v1")
            .training(replay_buffer_config={
                "type": "PrioritizedEpisodeReplayBuffer",
                "capacity": 60000,
                "alpha": 0.5,
                "beta": 0.5,
            })
            .env_runners(num_env_runners=1)
        )
        algo = config.build()
        algo.train()
        algo.stop()

    .. testcode::

        from ray.rllib.algorithms.dqn.dqn import DQNConfig
        from ray import tune

        config = (
            DQNConfig()
            .environment("CartPole-v1")
            .training(
                num_atoms=tune.grid_search([1,])
            )
        )
        tune.Tuner(
            "DQN",
            run_config=tune.RunConfig(stop={"training_iteration":1}),
            param_space=config,
        ).fit()

    .. testoutput::
        :hide:

        ...


    """

    def __init__(self, algo_class=None):
        """Initializes a DQNConfig instance."""
        self.exploration_config = {
            "type": "EpsilonGreedy",
            "initial_epsilon": 1.0,
            "final_epsilon": 0.02,
            "epsilon_timesteps": 10000,
        }

        super().__init__(algo_class=algo_class or DQN)

        # Overrides of AlgorithmConfig defaults
        # `env_runners()`
        # Set to `self.n_step`, if 'auto'.
        self.rollout_fragment_length: Union[int, str] = "auto"
        # New stack uses `epsilon` as either a constant value or a scheduler
        # defined like this.
        # TODO (simon): Ensure that users can understand how to provide epsilon.
        #  (sven): Should we add this to `self.env_runners(epsilon=..)`?
        self.epsilon = [(0, 1.0), (10000, 0.05)]

        # `training()`
        self.grad_clip = 40.0
        # Note: Only when using enable_rl_module_and_learner=True can the clipping mode
        # be configured by the user. On the old API stack, RLlib will always clip by
        # global_norm, no matter the value of `grad_clip_by`.
        self.grad_clip_by = "global_norm"
        self.lr = 5e-4
        self.train_batch_size = 32

        # `evaluation()`
        self.evaluation(evaluation_config=AlgorithmConfig.overrides(explore=False))

        # `reporting()`
        self.min_time_s_per_iteration = None
        self.min_sample_timesteps_per_iteration = 1000

        # DQN specific config settings.
        # fmt: off
        # __sphinx_doc_begin__
        self.target_network_update_freq = 500
        self.num_steps_sampled_before_learning_starts = 1000
        self.store_buffer_in_checkpoints = False
        self.adam_epsilon = 1e-8

        self.tau = 1.0

        self.num_atoms = 1
        self.v_min = -10.0
        self.v_max = 10.0
        self.noisy = False
        self.sigma0 = 0.5
        self.dueling = True
        self.hiddens = [256]
        self.double_q = True
        self.n_step = 1
        self.before_learn_on_batch = None
        self.training_intensity = None
        self.td_error_loss_fn = "huber"
        self.categorical_distribution_temperature = 1.0
        # The burn-in for stateful `RLModule`s.
        self.burn_in_len = 0

        # Replay buffer configuration.
        self.replay_buffer_config = {
            "type": "PrioritizedEpisodeReplayBuffer",
            # Size of the replay buffer. Note that if async_updates is set,
            # then each worker will have a replay buffer of this size.
            "capacity": 50000,
            "alpha": 0.6,
            # Beta parameter for sampling from prioritized replay buffer.
            "beta": 0.4,
        }
        # fmt: on
        # __sphinx_doc_end__

        self.lr_schedule = None  # @OldAPIStack

        # Deprecated
        self.buffer_size = DEPRECATED_VALUE
        self.prioritized_replay = DEPRECATED_VALUE
        self.learning_starts = DEPRECATED_VALUE
        self.replay_batch_size = DEPRECATED_VALUE
        # Can not use DEPRECATED_VALUE here because -1 is a common config value
        self.replay_sequence_length = None
        self.prioritized_replay_alpha = DEPRECATED_VALUE
        self.prioritized_replay_beta = DEPRECATED_VALUE
        self.prioritized_replay_eps = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        target_network_update_freq: Optional[int] = NotProvided,
        replay_buffer_config: Optional[dict] = NotProvided,
        store_buffer_in_checkpoints: Optional[bool] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        epsilon: Optional[LearningRateOrSchedule] = NotProvided,
        adam_epsilon: Optional[float] = NotProvided,
        grad_clip: Optional[int] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        tau: Optional[float] = NotProvided,
        num_atoms: Optional[int] = NotProvided,
        v_min: Optional[float] = NotProvided,
        v_max: Optional[float] = NotProvided,
        noisy: Optional[bool] = NotProvided,
        sigma0: Optional[float] = NotProvided,
        dueling: Optional[bool] = NotProvided,
        hiddens: Optional[int] = NotProvided,
        double_q: Optional[bool] = NotProvided,
        n_step: Optional[Union[int, Tuple[int, int]]] = NotProvided,
        before_learn_on_batch: Callable[
            [Type[MultiAgentBatch], List[Type[Policy]], Type[int]],
            Type[MultiAgentBatch],
        ] = NotProvided,
        training_intensity: Optional[float] = NotProvided,
        td_error_loss_fn: Optional[str] = NotProvided,
        categorical_distribution_temperature: Optional[float] = NotProvided,
        burn_in_len: Optional[int] = NotProvided,
        **kwargs,
    ) -> Self:
        """Sets the training related configuration.

        Args:
            target_network_update_freq: Update the target network every
                `target_network_update_freq` sample steps.
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "capacity": 50000,
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
            store_buffer_in_checkpoints: Set this to True, if you want the contents of
                your buffer(s) to be stored in any saved checkpoints as well.
                Warnings will be created if:
                - This is True AND restoring from a checkpoint that contains no buffer
                data.
                - This is False AND restoring from a checkpoint that does contain
                buffer data.
            epsilon: Epsilon exploration schedule. In the format of [[timestep, value],
                [timestep, value], ...]. A schedule must start from
                timestep 0.
            adam_epsilon: Adam optimizer's epsilon hyper parameter.
            grad_clip: If not None, clip gradients during optimization at this value.
            num_steps_sampled_before_learning_starts: Number of timesteps to collect
                from rollout workers before we start sampling from replay buffers for
                learning. Whether we count this in agent steps or environment steps
                depends on config.multi_agent(count_steps_by=..).
            tau: Update the target by \tau * policy + (1-\tau) * target_policy.
            num_atoms: Number of atoms for representing the distribution of return.
                When this is greater than 1, distributional Q-learning is used.
            v_min: Minimum value estimation
            v_max: Maximum value estimation
            noisy: Whether to use noisy network to aid exploration. This adds parametric
                noise to the model weights.
            sigma0: Control the initial parameter noise for noisy nets.
            dueling: Whether to use dueling DQN.
            hiddens: Dense-layer setup for each the advantage branch and the value
                branch
            double_q: Whether to use double DQN.
            n_step: N-step target updates. If >1, sars' tuples in trajectories will be
                postprocessed to become sa[discounted sum of R][s t+n] tuples. An
                integer will be interpreted as a fixed n-step value. If a tuple of 2
                ints is provided here, the n-step value will be drawn for each sample(!)
                in the train batch from a uniform distribution over the closed interval
                defined by `[n_step[0], n_step[1]]`.
            before_learn_on_batch: Callback to run before learning on a multi-agent
                batch of experiences.
            training_intensity: The intensity with which to update the model (vs
                collecting samples from the env).
                If None, uses "natural" values of:
                `train_batch_size` / (`rollout_fragment_length` x `num_env_runners` x
                `num_envs_per_env_runner`).
                If not None, will make sure that the ratio between timesteps inserted
                into and sampled from the buffer matches the given values.
                Example:
                training_intensity=1000.0
                train_batch_size=250
                rollout_fragment_length=1
                num_env_runners=1 (or 0)
                num_envs_per_env_runner=1
                -> natural value = 250 / 1 = 250.0
                -> will make sure that replay+train op will be executed 4x asoften as
                rollout+insert op (4 * 250 = 1000).
                See: rllib/algorithms/dqn/dqn.py::calculate_rr_weights for further
                details.
            td_error_loss_fn: "huber" or "mse". loss function for calculating TD error
                when num_atoms is 1. Note that if num_atoms is > 1, this parameter
                is simply ignored, and softmax cross entropy loss will be used.
            categorical_distribution_temperature: Set the temperature parameter used
                by Categorical action distribution. A valid temperature is in the range
                of [0, 1]. Note that this mostly affects evaluation since TD error uses
                argmax for return calculation.
            burn_in_len: The burn-in period for a stateful RLModule. It allows the
                Learner to utilize the initial `burn_in_len` steps in a replay sequence
                solely for unrolling the network and establishing a typical starting
                state. The network is then updated on the remaining steps of the
                sequence. This process helps mitigate issues stemming from a poor
                initial state - zero or an outdated recorded state. Consider setting
                this parameter to a positive integer if your stateful RLModule faces
                convergence challenges or exhibits signs of catastrophic forgetting.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if replay_buffer_config is not NotProvided:
            # Override entire `replay_buffer_config` if `type` key changes.
            # Update, if `type` key remains the same or is not specified.
            new_replay_buffer_config = deep_update(
                {"replay_buffer_config": self.replay_buffer_config},
                {"replay_buffer_config": replay_buffer_config},
                False,
                ["replay_buffer_config"],
                ["replay_buffer_config"],
            )
            self.replay_buffer_config = new_replay_buffer_config["replay_buffer_config"]
        if store_buffer_in_checkpoints is not NotProvided:
            self.store_buffer_in_checkpoints = store_buffer_in_checkpoints
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if epsilon is not NotProvided:
            self.epsilon = epsilon
        if adam_epsilon is not NotProvided:
            self.adam_epsilon = adam_epsilon
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )
        if tau is not NotProvided:
            self.tau = tau
        if num_atoms is not NotProvided:
            self.num_atoms = num_atoms
        if v_min is not NotProvided:
            self.v_min = v_min
        if v_max is not NotProvided:
            self.v_max = v_max
        if noisy is not NotProvided:
            self.noisy = noisy
        if sigma0 is not NotProvided:
            self.sigma0 = sigma0
        if dueling is not NotProvided:
            self.dueling = dueling
        if hiddens is not NotProvided:
            self.hiddens = hiddens
        if double_q is not NotProvided:
            self.double_q = double_q
        if n_step is not NotProvided:
            self.n_step = n_step
        if before_learn_on_batch is not NotProvided:
            self.before_learn_on_batch = before_learn_on_batch
        if training_intensity is not NotProvided:
            self.training_intensity = training_intensity
        if td_error_loss_fn is not NotProvided:
            self.td_error_loss_fn = td_error_loss_fn
        if categorical_distribution_temperature is not NotProvided:
            self.categorical_distribution_temperature = (
                categorical_distribution_temperature
            )
        if burn_in_len is not NotProvided:
            self.burn_in_len = burn_in_len

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.enable_rl_module_and_learner:
            # `lr_schedule` checking.
            if self.lr_schedule is not None:
                self._value_error(
                    "`lr_schedule` is deprecated and must be None! Use the "
                    "`lr` setting to setup a schedule."
                )
        else:
            if not self.in_evaluation:
                validate_buffer_config(self)

            # TODO (simon): Find a clean solution to deal with configuration configs
            #  when using the new API stack.
            if self.exploration_config["type"] == "ParameterNoise":
                if self.batch_mode != "complete_episodes":
                    self._value_error(
                        "ParameterNoise Exploration requires `batch_mode` to be "
                        "'complete_episodes'. Try setting `config.env_runners("
                        "batch_mode='complete_episodes')`."
                    )
                if self.noisy:
                    self._value_error(
                        "ParameterNoise Exploration and `noisy` network cannot be"
                        " used at the same time!"
                    )

        if self.td_error_loss_fn not in ["huber", "mse"]:
            self._value_error("`td_error_loss_fn` must be 'huber' or 'mse'!")

        # Check rollout_fragment_length to be compatible with n_step.
        if (
            not self.in_evaluation
            and self.rollout_fragment_length != "auto"
            and self.rollout_fragment_length < self.n_step
        ):
            self._value_error(
                f"Your `rollout_fragment_length` ({self.rollout_fragment_length}) is "
                f"smaller than `n_step` ({self.n_step})! "
                "Try setting config.env_runners(rollout_fragment_length="
                f"{self.n_step})."
            )

        # Check, if the `max_seq_len` is longer then the burn-in.
        if (
            "max_seq_len" in self.model_config
            and 0 < self.model_config["max_seq_len"] <= self.burn_in_len
        ):
            raise ValueError(
                f"Your defined `burn_in_len`={self.burn_in_len} is larger or equal "
                f"`max_seq_len`={self.model_config['max_seq_len']}! Either decrease "
                "the `burn_in_len` or increase your `max_seq_len`."
            )

        # Validate that we use the corresponding `EpisodeReplayBuffer` when using
        # episodes.
        # TODO (sven, simon): Implement the multi-agent case for replay buffers.
        from ray.rllib.utils.replay_buffers.episode_replay_buffer import (
            EpisodeReplayBuffer,
        )

        if (
            self.enable_env_runner_and_connector_v2
            and not isinstance(self.replay_buffer_config["type"], str)
            and not issubclass(self.replay_buffer_config["type"], EpisodeReplayBuffer)
        ):
            self._value_error(
                "When using the new `EnvRunner API` the replay buffer must be of type "
                "`EpisodeReplayBuffer`."
            )
        elif not self.enable_env_runner_and_connector_v2 and (
            (
                isinstance(self.replay_buffer_config["type"], str)
                and "Episode" in self.replay_buffer_config["type"]
            )
            or issubclass(self.replay_buffer_config["type"], EpisodeReplayBuffer)
        ):
            self._value_error(
                "When using the old API stack the replay buffer must not be of type "
                "`EpisodeReplayBuffer`! We suggest you use the following config to run "
                "DQN on the old API stack: `config.training(replay_buffer_config={"
                "'type': 'MultiAgentPrioritizedReplayBuffer', "
                "'prioritized_replay_alpha': [alpha], "
                "'prioritized_replay_beta': [beta], "
                "'prioritized_replay_eps': [eps], "
                "})`."
            )

    @override(AlgorithmConfig)
    def get_rollout_fragment_length(self, worker_index: int = 0) -> int:
        if self.rollout_fragment_length == "auto":
            return (
                self.n_step[1]
                if isinstance(self.n_step, (tuple, list))
                else self.n_step
            )
        else:
            return self.rollout_fragment_length

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpecType:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.dqn.torch.default_dqn_torch_rl_module import (
                DefaultDQNTorchRLModule,
            )

            return RLModuleSpec(
                module_class=DefaultDQNTorchRLModule,
                model_config=self.model_config,
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported! "
                "Use `config.framework('torch')` instead."
            )

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self) -> Dict[str, Any]:
        return super()._model_config_auto_includes | {
            "double_q": self.double_q,
            "dueling": self.dueling,
            "epsilon": self.epsilon,
            "num_atoms": self.num_atoms,
            "std_init": self.sigma0,
            "v_max": self.v_max,
            "v_min": self.v_min,
        }

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.dqn.torch.dqn_torch_learner import (
                DQNTorchLearner,
            )

            return DQNTorchLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported! "
                "Use `config.framework('torch')` instead."
            )


def calculate_rr_weights(config: AlgorithmConfig) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config.training_intensity:
        return [1, 1]

    # Calculate the "native ratio" as:
    # [train-batch-size] / [size of env-rolled-out sampled data]
    # This is to set freshly rollout-collected data in relation to
    # the data we pull from the replay buffer (which also contains old
    # samples).
    native_ratio = config.total_train_batch_size / (
        config.get_rollout_fragment_length()
        * config.num_envs_per_env_runner
        # Add one to workers because the local
        # worker usually collects experiences as well, and we avoid division by zero.
        * max(config.num_env_runners + 1, 1)
    )

    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    sample_and_train_weight = config.training_intensity / native_ratio
    if sample_and_train_weight < 1:
        return [int(np.round(1 / sample_and_train_weight)), 1]
    else:
        return [1, int(np.round(sample_and_train_weight))]


class DQN(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> DQNConfig:
        return DQNConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return DQNTorchPolicy
        else:
            return DQNTFPolicy

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig) -> None:
        super().setup(config)

        if self.config.enable_env_runner_and_connector_v2 and self.env_runner_group:
            if self.env_runner is None:
                self._module_is_stateful = self.env_runner_group.foreach_env_runner(
                    lambda er: er.module.is_stateful(),
                    remote_worker_ids=[1],
                    local_env_runner=False,
                )[0]
            else:
                self._module_is_stateful = self.env_runner.module.is_stateful()

    @override(Algorithm)
    def training_step(self) -> None:
        """DQN training iteration function.

        Each training iteration, we:
        - Sample (MultiAgentBatch) from workers.
        - Store new samples in replay buffer.
        - Sample training batch (MultiAgentBatch) from replay buffer.
        - Learn on training batch.
        - Update remote workers' new policy weights.
        - Update target network every `target_network_update_freq` sample steps.
        - Return all collected metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        # Old API stack (Policy, RolloutWorker, Connector).
        if not self.config.enable_env_runner_and_connector_v2:
            return self._training_step_old_api_stack()

        # New API stack (RLModule, Learner, EnvRunner, ConnectorV2).
        return self._training_step_new_api_stack()

    def _training_step_new_api_stack(self):
        # Alternate between storing and sampling and training.
        store_weight, sample_and_train_weight = calculate_rr_weights(self.config)

        # Run multiple sampling + storing to buffer iterations.
        for _ in range(store_weight):
            with self.metrics.log_time((TIMERS, ENV_RUNNER_SAMPLING_TIMER)):
                # Sample in parallel from workers.
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    concat=True,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=True,
                    _return_metrics=True,
                )
            # Reduce EnvRunner metrics over the n EnvRunners.
            self.metrics.aggregate(env_runner_results, key=ENV_RUNNER_RESULTS)

            # Add the sampled experiences to the replay buffer.
            with self.metrics.log_time((TIMERS, REPLAY_BUFFER_ADD_DATA_TIMER)):
                self.local_replay_buffer.add(episodes)

        if self.config.count_steps_by == "agent_steps":
            current_ts = sum(
                self.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_AGENT_STEPS_SAMPLED_LIFETIME), default={}
                ).values()
            )
        else:
            current_ts = self.metrics.peek(
                (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
            )

        # If enough experiences have been sampled start training.
        if current_ts >= self.config.num_steps_sampled_before_learning_starts:
            # Run multiple sample-from-buffer and update iterations.
            for _ in range(sample_and_train_weight):
                # Sample a list of episodes used for learning from the replay buffer.
                with self.metrics.log_time((TIMERS, REPLAY_BUFFER_SAMPLE_TIMER)):

                    episodes = self.local_replay_buffer.sample(
                        num_items=self.config.total_train_batch_size,
                        n_step=self.config.n_step,
                        # In case an `EpisodeReplayBuffer` is used we need to provide
                        # the sequence length.
                        batch_length_T=(
                            self._module_is_stateful
                            * self.config.model_config.get("max_seq_len", 0)
                        ),
                        lookback=int(self._module_is_stateful),
                        # TODO (simon): Implement `burn_in_len` in SAC and remove this
                        # if-else clause.
                        min_batch_length_T=self.config.burn_in_len
                        if hasattr(self.config, "burn_in_len")
                        else 0,
                        gamma=self.config.gamma,
                        beta=self.config.replay_buffer_config.get("beta"),
                        sample_episodes=True,
                    )

                    # Get the replay buffer metrics.
                    replay_buffer_results = self.local_replay_buffer.get_metrics()
                    self.metrics.aggregate(
                        [replay_buffer_results], key=REPLAY_BUFFER_RESULTS
                    )

                # Perform an update on the buffer-sampled train batch.
                with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
                    learner_results = self.learner_group.update(
                        episodes=episodes,
                        timesteps={
                            NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                                self.metrics.peek(
                                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                                )
                            ),
                            NUM_AGENT_STEPS_SAMPLED_LIFETIME: (
                                self.metrics.peek(
                                    (
                                        ENV_RUNNER_RESULTS,
                                        NUM_AGENT_STEPS_SAMPLED_LIFETIME,
                                    )
                                )
                            ),
                        },
                    )
                    # Isolate TD-errors from result dicts (we should not log these to
                    # disk or WandB, they might be very large).
                    td_errors = defaultdict(list)
                    for res in learner_results:
                        for module_id, module_results in res.items():
                            if TD_ERROR_KEY in module_results:
                                td_errors[module_id].extend(
                                    convert_to_numpy(
                                        module_results.pop(TD_ERROR_KEY).peek()
                                    )
                                )
                    td_errors = {
                        module_id: {TD_ERROR_KEY: np.concatenate(s, axis=0)}
                        for module_id, s in td_errors.items()
                    }
                    self.metrics.aggregate(learner_results, key=LEARNER_RESULTS)

                # Update replay buffer priorities.
                with self.metrics.log_time((TIMERS, REPLAY_BUFFER_UPDATE_PRIOS_TIMER)):
                    update_priorities_in_episode_replay_buffer(
                        replay_buffer=self.local_replay_buffer,
                        td_errors=td_errors,
                    )

            # Update weights and global_vars - after learning on the local worker -
            # on all remote workers.
            with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
                modules_to_update = set(learner_results[0].keys()) - {ALL_MODULES}
                # NOTE: the new API stack does not use global vars.
                self.env_runner_group.sync_weights(
                    from_worker_or_learner_group=self.learner_group,
                    policies=modules_to_update,
                    global_vars=None,
                    inference_only=True,
                )

    def _training_step_old_api_stack(self) -> ResultDict:
        """Training step for the old API stack.

        More specifically this training step relies on `RolloutWorker`.
        """
        train_results = {}

        # We alternate between storing new samples and sampling and training
        store_weight, sample_and_train_weight = calculate_rr_weights(self.config)

        for _ in range(store_weight):
            # Sample (MultiAgentBatch) from workers.
            with self._timers[SAMPLE_TIMER]:
                new_sample_batch: SampleBatchType = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    concat=True,
                    sample_timeout_s=self.config.sample_timeout_s,
                )

            # Return early if all our workers failed.
            if not new_sample_batch:
                return {}

            # Update counters
            self._counters[NUM_AGENT_STEPS_SAMPLED] += new_sample_batch.agent_steps()
            self._counters[NUM_ENV_STEPS_SAMPLED] += new_sample_batch.env_steps()

            # Store new samples in replay buffer.
            self.local_replay_buffer.add(new_sample_batch)

        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }

        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            (
                NUM_AGENT_STEPS_SAMPLED
                if self.config.count_steps_by == "agent_steps"
                else NUM_ENV_STEPS_SAMPLED
            )
        ]

        if cur_ts > self.config.num_steps_sampled_before_learning_starts:
            for _ in range(sample_and_train_weight):
                # Sample training batch (MultiAgentBatch) from replay buffer.
                train_batch = sample_min_n_steps_from_buffer(
                    self.local_replay_buffer,
                    self.config.total_train_batch_size,
                    count_by_agent_steps=self.config.count_steps_by == "agent_steps",
                )

                # Postprocess batch before we learn on it
                post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
                train_batch = post_fn(train_batch, self.env_runner_group, self.config)

                # Learn on training batch.
                # Use simple optimizer (only for multi-agent or tf-eager; all other
                # cases should use the multi-GPU optimizer, even if only using 1 GPU)
                if self.config.get("simple_optimizer") is True:
                    train_results = train_one_step(self, train_batch)
                else:
                    train_results = multi_gpu_train_one_step(self, train_batch)

                # Update replay buffer priorities.
                update_priorities_in_replay_buffer(
                    self.local_replay_buffer,
                    self.config,
                    train_batch,
                    train_results,
                )

                last_update = self._counters[LAST_TARGET_UPDATE_TS]
                if cur_ts - last_update >= self.config.target_network_update_freq:
                    to_update = self.env_runner.get_policies_to_train()
                    self.env_runner.foreach_policy_to_train(
                        lambda p, pid, to_update=to_update: (
                            pid in to_update and p.update_target()
                        )
                    )
                    self._counters[NUM_TARGET_UPDATES] += 1
                    self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

                # Update weights and global_vars - after learning on the local worker -
                # on all remote workers.
                with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                    self.env_runner_group.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results
