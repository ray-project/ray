"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""

import logging
from typing import Any, Dict, Optional, Union

import gymnasium as gym
from typing_extensions import Self

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dreamerv3.dreamerv3_catalog import DreamerV3Catalog
from ray.rllib.algorithms.dreamerv3.utils import do_symlog_obs
from ray.rllib.algorithms.dreamerv3.utils.add_is_firsts_to_batch import (
    AddIsFirstsToBatch,
)
from ray.rllib.algorithms.dreamerv3.utils.summaries import (
    report_dreamed_eval_trajectory_vs_samples,
    report_predicted_vs_sampled_obs,
    report_sampling_and_replay_buffer,
)
from ray.rllib.connectors.common import AddStatesFromEpisodesToBatch
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env import INPUT_ENV_SINGLE_SPACES
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import deep_update
from ray.rllib.utils.annotations import PublicAPI, override
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    LEARN_ON_BATCH_TIMER,
    LEARNER_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_GRAD_UPDATES_LIFETIME,
    NUM_SYNCH_WORKER_WEIGHTS,
    REPLAY_BUFFER_RESULTS,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
)
from ray.rllib.utils.numpy import one_hot
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.typing import LearningRateOrSchedule

logger = logging.getLogger(__name__)


class DreamerV3Config(AlgorithmConfig):
    """Defines a configuration class from which a DreamerV3 can be built.

    .. testcode::

        from ray.rllib.algorithms.dreamerv3 import DreamerV3Config
        config = (
            DreamerV3Config()
            .environment("CartPole-v1")
            .training(
                model_size="XS",
                training_ratio=1,
                # TODO
                model={
                    "batch_size_B": 1,
                    "batch_length_T": 1,
                    "horizon_H": 1,
                    "gamma": 0.997,
                    "model_size": "XS",
                },
            )
        )

        config = config.learners(num_learners=0)
        # Build a Algorithm object from the config and run 1 training iteration.
        algo = config.build()
        # algo.train()
        del algo
    """

    def __init__(self, algo_class=None):
        """Initializes a DreamerV3Config instance."""
        super().__init__(algo_class=algo_class or DreamerV3)

        # fmt: off
        # __sphinx_doc_begin__

        # DreamerV3 specific settings:
        self.model_size = "XS"
        self.training_ratio = 1024

        self.replay_buffer_config = {
            "type": "EpisodeReplayBuffer",
            "capacity": int(1e6),
        }
        self.world_model_lr = 1e-4
        self.actor_lr = 3e-5
        self.critic_lr = 3e-5
        self.batch_size_B = 16
        self.batch_length_T = 64
        self.horizon_H = 15
        self.gae_lambda = 0.95  # [1] eq. 7.
        self.entropy_scale = 3e-4  # [1] eq. 11.
        self.return_normalization_decay = 0.99  # [1] eq. 11 and 12.
        self.train_critic = True
        self.train_actor = True
        self.intrinsic_rewards_scale = 0.1
        self.world_model_grad_clip_by_global_norm = 1000.0
        self.critic_grad_clip_by_global_norm = 100.0
        self.actor_grad_clip_by_global_norm = 100.0
        self.symlog_obs = "auto"
        self.use_float16 = False
        self.use_curiosity = False

        # Reporting.
        # DreamerV3 is super sample efficient and only needs very few episodes
        # (normally) to learn. Leaving this at its default value would gravely
        # underestimate the learning performance over the course of an experiment.
        self.metrics_num_episodes_for_smoothing = 1
        self.report_individual_batch_item_stats = False
        self.report_dream_data = False
        self.report_images_and_videos = False

        # Override some of AlgorithmConfig's default values with DreamerV3-specific
        # values.
        self.lr = None
        self.gamma = 0.997  # [1] eq. 7.
        # Do not use! Set `batch_size_B` and `batch_length_T` instead.
        self.train_batch_size = None
        self.num_env_runners = 0
        self.rollout_fragment_length = 1
        # Dreamer only runs on the new API stack.
        self.enable_rl_module_and_learner = True
        self.enable_env_runner_and_connector_v2 = True
        # TODO (sven): DreamerV3 still uses its own EnvRunner class. This env-runner
        #  does not use connectors. We therefore should not attempt to merge/broadcast
        #  the connector states between EnvRunners (if >0). Note that this is only
        #  relevant if num_env_runners > 0, which is normally not the case when using
        #  this algo.
        self.use_worker_filter_stats = False
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def build_env_to_module_connector(self, env, spaces, device):
        connector = super().build_env_to_module_connector(env, spaces, device)

        # Prepend the "is_first" connector such that the RSSM knows, when to insert
        # its (learned) internal state into the batch.
        # We have to do this before the `AddStatesFromEpisodesToBatch` piece
        # such that the column is properly batched/time-ranked.
        if self.add_default_connectors_to_learner_pipeline:
            connector.insert_before(
                AddStatesFromEpisodesToBatch,
                AddIsFirstsToBatch(),
            )
        return connector

    @property
    def batch_size_B_per_learner(self):
        """Returns the batch_size_B per Learner worker.

        Needed by some of the DreamerV3 loss math."""
        return self.batch_size_B // (self.num_learners or 1)

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        model_size: Optional[str] = NotProvided,
        training_ratio: Optional[float] = NotProvided,
        batch_size_B: Optional[int] = NotProvided,
        batch_length_T: Optional[int] = NotProvided,
        horizon_H: Optional[int] = NotProvided,
        gae_lambda: Optional[float] = NotProvided,
        entropy_scale: Optional[float] = NotProvided,
        return_normalization_decay: Optional[float] = NotProvided,
        train_critic: Optional[bool] = NotProvided,
        train_actor: Optional[bool] = NotProvided,
        intrinsic_rewards_scale: Optional[float] = NotProvided,
        world_model_lr: Optional[LearningRateOrSchedule] = NotProvided,
        actor_lr: Optional[LearningRateOrSchedule] = NotProvided,
        critic_lr: Optional[LearningRateOrSchedule] = NotProvided,
        world_model_grad_clip_by_global_norm: Optional[float] = NotProvided,
        critic_grad_clip_by_global_norm: Optional[float] = NotProvided,
        actor_grad_clip_by_global_norm: Optional[float] = NotProvided,
        symlog_obs: Optional[Union[bool, str]] = NotProvided,
        use_float16: Optional[bool] = NotProvided,
        replay_buffer_config: Optional[dict] = NotProvided,
        use_curiosity: Optional[bool] = NotProvided,
        **kwargs,
    ) -> Self:
        """Sets the training related configuration.

        Args:
            model_size: The main switch for adjusting the overall model size. See [1]
                (table B) for more information on the effects of this setting on the
                model architecture.
                Supported values are "XS", "S", "M", "L", "XL" (as per the paper), as
                well as, "nano", "micro", "mini", and "XXS" (for RLlib's
                implementation). See ray.rllib.algorithms.dreamerv3.utils.
                __init__.py for the details on what exactly each size does to the layer
                sizes, number of layers, etc..
            training_ratio: The ratio of total steps trained (sum of the sizes of all
                batches ever sampled from the replay buffer) over the total env steps
                taken (in the actual environment, not the dreamed one). For example,
                if the training_ratio is 1024 and the batch size is 1024, we would take
                1 env step for every training update: 1024 / 1. If the training ratio
                is 512 and the batch size is 1024, we would take 2 env steps and then
                perform a single training update (on a 1024 batch): 1024 / 2.
            batch_size_B: The batch size (B) interpreted as number of rows (each of
                length `batch_length_T`) to sample from the replay buffer in each
                iteration.
            batch_length_T: The batch length (T) interpreted as the length of each row
                sampled from the replay buffer in each iteration. Note that
                `batch_size_B` rows will be sampled in each iteration. Rows normally
                contain consecutive data (consecutive timesteps from the same episode),
                but there might be episode boundaries in a row as well.
            horizon_H: The horizon (in timesteps) used to create dreamed data from the
                world model, which in turn is used to train/update both actor- and
                critic networks.
            gae_lambda: The lambda parameter used for computing the GAE-style
                value targets for the actor- and critic losses.
            entropy_scale: The factor with which to multiply the entropy loss term
                inside the actor loss.
            return_normalization_decay: The decay value to use when computing the
                running EMA values for return normalization (used in the actor loss).
            train_critic: Whether to train the critic network. If False, `train_actor`
                must also be False (cannot train actor w/o training the critic).
            train_actor: Whether to train the actor network. If True, `train_critic`
                must also be True (cannot train actor w/o training the critic).
            intrinsic_rewards_scale: The factor to multiply intrinsic rewards with
                before adding them to the extrinsic (environment) rewards.
            world_model_lr: The learning rate or schedule for the world model optimizer.
            actor_lr: The learning rate or schedule for the actor optimizer.
            critic_lr: The learning rate or schedule for the critic optimizer.
            world_model_grad_clip_by_global_norm: World model grad clipping value
                (by global norm).
            critic_grad_clip_by_global_norm: Critic grad clipping value
                (by global norm).
            actor_grad_clip_by_global_norm: Actor grad clipping value (by global norm).
            symlog_obs: Whether to symlog observations or not. If set to "auto"
                (default), will check for the environment's observation space and then
                only symlog if not an image space.
            use_float16: Whether to train with mixed float16 precision. In this mode,
                model parameters are stored as float32, but all computations are
                performed in float16 space (except for losses and distribution params
                and outputs).
            replay_buffer_config: Replay buffer config.
                Only serves in DreamerV3 to set the capacity of the replay buffer.
                Note though that in the paper ([1]) a size of 1M is used for all
                benchmarks and there doesn't seem to be a good reason to change this
                parameter.
                Examples:
                {
                "type": "EpisodeReplayBuffer",
                "capacity": 100000,
                }

        Returns:
            This updated AlgorithmConfig object.
        """
        # Not fully supported/tested yet.
        if use_curiosity is not NotProvided:
            raise ValueError(
                "`DreamerV3Config.curiosity` is not fully supported and tested yet! "
                "It thus remains disabled for now."
            )

        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if model_size is not NotProvided:
            self.model_size = model_size
        if training_ratio is not NotProvided:
            self.training_ratio = training_ratio
        if batch_size_B is not NotProvided:
            self.batch_size_B = batch_size_B
        if batch_length_T is not NotProvided:
            self.batch_length_T = batch_length_T
        if horizon_H is not NotProvided:
            self.horizon_H = horizon_H
        if gae_lambda is not NotProvided:
            self.gae_lambda = gae_lambda
        if entropy_scale is not NotProvided:
            self.entropy_scale = entropy_scale
        if return_normalization_decay is not NotProvided:
            self.return_normalization_decay = return_normalization_decay
        if train_critic is not NotProvided:
            self.train_critic = train_critic
        if train_actor is not NotProvided:
            self.train_actor = train_actor
        if intrinsic_rewards_scale is not NotProvided:
            self.intrinsic_rewards_scale = intrinsic_rewards_scale
        if world_model_lr is not NotProvided:
            self.world_model_lr = world_model_lr
        if actor_lr is not NotProvided:
            self.actor_lr = actor_lr
        if critic_lr is not NotProvided:
            self.critic_lr = critic_lr
        if world_model_grad_clip_by_global_norm is not NotProvided:
            self.world_model_grad_clip_by_global_norm = (
                world_model_grad_clip_by_global_norm
            )
        if critic_grad_clip_by_global_norm is not NotProvided:
            self.critic_grad_clip_by_global_norm = critic_grad_clip_by_global_norm
        if actor_grad_clip_by_global_norm is not NotProvided:
            self.actor_grad_clip_by_global_norm = actor_grad_clip_by_global_norm
        if symlog_obs is not NotProvided:
            self.symlog_obs = symlog_obs
        if use_float16 is not NotProvided:
            self.use_float16 = use_float16
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

        return self

    @override(AlgorithmConfig)
    def reporting(
        self,
        *,
        report_individual_batch_item_stats: Optional[bool] = NotProvided,
        report_dream_data: Optional[bool] = NotProvided,
        report_images_and_videos: Optional[bool] = NotProvided,
        **kwargs,
    ):
        """Sets the reporting related configuration.

        Args:
            report_individual_batch_item_stats: Whether to include loss and other stats
                per individual timestep inside the training batch in the result dict
                returned by `training_step()`. If True, besides the `CRITIC_L_total`,
                the individual critic loss values per batch row and time axis step
                in the train batch (CRITIC_L_total_B_T) will also be part of the
                results.
            report_dream_data:  Whether to include the dreamed trajectory data in the
                result dict returned by `training_step()`. If True, however, will
                slice each reported item in the dream data down to the shape.
                (H, B, t=0, ...), where H is the horizon and B is the batch size. The
                original time axis will only be represented by the first timestep
                to not make this data too large to handle.
            report_images_and_videos: Whether to include any image/video data in the
                result dict returned by `training_step()`.
            **kwargs:

        Returns:
            This updated AlgorithmConfig object.
        """
        super().reporting(**kwargs)

        if report_individual_batch_item_stats is not NotProvided:
            self.report_individual_batch_item_stats = report_individual_batch_item_stats
        if report_dream_data is not NotProvided:
            self.report_dream_data = report_dream_data
        if report_images_and_videos is not NotProvided:
            self.report_images_and_videos = report_images_and_videos

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call the super class' validation method first.
        super().validate()

        # Make sure, users are not using DreamerV3 yet for multi-agent:
        if self.is_multi_agent:
            self._value_error("DreamerV3 does NOT support multi-agent setups yet!")

        # Make sure, we are configure for the new API stack.
        if not self.enable_rl_module_and_learner:
            self._value_error(
                "DreamerV3 must be run with `config.api_stack("
                "enable_rl_module_and_learner=True)`!"
            )

        # If run on several Learners, the provided batch_size_B must be a multiple
        # of `num_learners`.
        if self.num_learners > 1 and (self.batch_size_B % self.num_learners != 0):
            self._value_error(
                f"Your `batch_size_B` ({self.batch_size_B}) must be a multiple of "
                f"`num_learners` ({self.num_learners}) in order for "
                "DreamerV3 to be able to split batches evenly across your Learner "
                "processes."
            )

        # Cannot train actor w/o critic.
        if self.train_actor and not self.train_critic:
            self._value_error(
                "Cannot train actor network (`train_actor=True`) w/o training critic! "
                "Make sure you either set `train_critic=True` or `train_actor=False`."
            )
        # Use DreamerV3 specific batch size settings.
        if self.train_batch_size is not None:
            self._value_error(
                "`train_batch_size` should NOT be set! Use `batch_size_B` and "
                "`batch_length_T` instead."
            )
        # Must be run with `EpisodeReplayBuffer` type.
        if self.replay_buffer_config.get("type") != "EpisodeReplayBuffer":
            self._value_error(
                "DreamerV3 must be run with the `EpisodeReplayBuffer` type! None "
                "other supported."
            )

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "torch":
            from ray.rllib.algorithms.dreamerv3.torch.dreamerv3_torch_learner import (
                DreamerV3TorchLearner,
            )

            return DreamerV3TorchLearner
        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.dreamerv3.torch.dreamerv3_torch_rl_module import (
                DreamerV3TorchRLModule as module,
            )

        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")

        return RLModuleSpec(module_class=module, catalog_class=DreamerV3Catalog)

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self) -> Dict[str, Any]:
        return super()._model_config_auto_includes | {
            "gamma": self.gamma,
            "horizon_H": self.horizon_H,
            "model_size": self.model_size,
            "symlog_obs": self.symlog_obs,
            "use_float16": self.use_float16,
            "batch_length_T": self.batch_length_T,
        }


class DreamerV3(Algorithm):
    """Implementation of the model-based DreamerV3 RL algorithm described in [1]."""

    # TODO (sven): Deprecate/do-over the Algorithm.compute_single_action() API.
    @override(Algorithm)
    def compute_single_action(self, *args, **kwargs):
        raise NotImplementedError(
            "DreamerV3 does not support the `compute_single_action()` API. Refer to the"
            " README here (https://github.com/ray-project/ray/tree/master/rllib/"
            "algorithms/dreamerv3) to find more information on how to run action "
            "inference with this algorithm."
        )

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> DreamerV3Config:
        return DreamerV3Config()

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        # Share RLModule between EnvRunner and single (local) Learner instance.
        # To avoid possibly expensive weight synching step.
        # if self.config.share_module_between_env_runner_and_learner:
        #    assert self.env_runner.module is None
        #    self.env_runner.module = self.learner_group._learner.module[
        #        DEFAULT_MODULE_ID
        #    ]

        # Create a replay buffer for storing actual env samples.
        self.replay_buffer = EpisodeReplayBuffer(
            capacity=self.config.replay_buffer_config["capacity"],
            batch_size_B=self.config.batch_size_B,
            batch_length_T=self.config.batch_length_T,
        )

    @override(Algorithm)
    def training_step(self) -> None:
        # Push enough samples into buffer initially before we start training.
        if self.training_iteration == 0:
            logger.info(
                "Filling replay buffer so it contains at least "
                f"{self.config.batch_size_B * self.config.batch_length_T} timesteps "
                "(required for a single train batch)."
            )

        # Have we sampled yet in this `training_step()` call?
        have_sampled = False
        with self.metrics.log_time((TIMERS, SAMPLE_TIMER)):
            # Continue sampling from the actual environment (and add collected samples
            # to our replay buffer) as long as we:
            while (
                # a) Don't have at least batch_size_B x batch_length_T timesteps stored
                # in the buffer. This is the minimum needed to train.
                self.replay_buffer.get_num_timesteps()
                < (self.config.batch_size_B * self.config.batch_length_T)
                # b) The computed `training_ratio` is >= the configured (desired)
                # training ratio (meaning we should continue sampling).
                or self.training_ratio >= self.config.training_ratio
                # c) we have not sampled at all yet in this `training_step()` call.
                or not have_sampled
            ):
                # Sample using the env runner's module.
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_agent_steps=(
                        self.config.rollout_fragment_length
                        * self.config.num_envs_per_env_runner
                    ),
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=True,
                    _return_metrics=True,
                )
                self.metrics.aggregate(env_runner_results, key=ENV_RUNNER_RESULTS)
                # Add ongoing and finished episodes into buffer. The buffer will
                # automatically take care of properly concatenating (by episode IDs)
                # the different chunks of the same episodes, even if they come in via
                # separate `add()` calls.
                self.replay_buffer.add(episodes=episodes)
                have_sampled = True

                # We took B x T env steps.
                env_steps_last_regular_sample = sum(len(eps) for eps in episodes)
                total_sampled = env_steps_last_regular_sample

                # If we have never sampled before (just started the algo and not
                # recovered from a checkpoint), sample B random actions first.
                if (
                    self.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME),
                        default=0,
                    )
                    == 0
                ):
                    _episodes, _env_runner_results = synchronous_parallel_sample(
                        worker_set=self.env_runner_group,
                        max_agent_steps=(
                            self.config.batch_size_B * self.config.batch_length_T
                            - env_steps_last_regular_sample
                        ),
                        sample_timeout_s=self.config.sample_timeout_s,
                        random_actions=True,
                        _uses_new_env_runners=True,
                        _return_metrics=True,
                    )
                    self.metrics.aggregate(_env_runner_results, key=ENV_RUNNER_RESULTS)
                    self.replay_buffer.add(episodes=_episodes)
                    total_sampled += sum(len(eps) for eps in _episodes)

        # Summarize environment interaction and buffer data.
        report_sampling_and_replay_buffer(
            metrics=self.metrics, replay_buffer=self.replay_buffer
        )
        # Get the replay buffer metrics.
        replay_buffer_results = self.local_replay_buffer.get_metrics()
        self.metrics.aggregate([replay_buffer_results], key=REPLAY_BUFFER_RESULTS)

        # Use self.spaces for the environment spaces of the env-runners
        single_observation_space, single_action_space = self.spaces[
            INPUT_ENV_SINGLE_SPACES
        ]

        # Continue sampling batch_size_B x batch_length_T sized batches from the buffer
        # and using these to update our models (`LearnerGroup.update()`)
        # until the computed `training_ratio` is larger than the configured one, meaning
        # we should go back and collect more samples again from the actual environment.
        # However, when calculating the `training_ratio` here, we use only the
        # trained steps in this very `training_step()` call over the most recent sample
        # amount (`env_steps_last_regular_sample`), not the global values. This is to
        # avoid a heavy overtraining at the very beginning when we have just pre-filled
        # the buffer with the minimum amount of samples.
        replayed_steps_this_iter = sub_iter = 0
        while (
            replayed_steps_this_iter / env_steps_last_regular_sample
        ) < self.config.training_ratio:
            # Time individual batch updates.
            with self.metrics.log_time((TIMERS, LEARN_ON_BATCH_TIMER)):
                logger.info(f"\tSub-iteration {self.training_iteration}/{sub_iter})")

                # Draw a new sample from the replay buffer.
                sample = self.replay_buffer.sample(
                    batch_size_B=self.config.batch_size_B,
                    batch_length_T=self.config.batch_length_T,
                )
                replayed_steps = self.config.batch_size_B * self.config.batch_length_T
                replayed_steps_this_iter += replayed_steps

                if isinstance(single_action_space, gym.spaces.Discrete):
                    sample["actions_ints"] = sample[Columns.ACTIONS]
                    sample[Columns.ACTIONS] = one_hot(
                        sample["actions_ints"],
                        depth=single_action_space.n,
                    )

                # Perform the actual update via our learner group.
                learner_results = self.learner_group.update(
                    batch=SampleBatch(sample).as_multi_agent(),
                    # TODO(sven): Maybe we should do this broadcase of global timesteps
                    #  at the end, like for EnvRunner global env step counts. Maybe when
                    #  we request the state from the Learners, we can - at the same
                    #  time - send the current globally summed/reduced-timesteps.
                    timesteps={
                        NUM_ENV_STEPS_SAMPLED_LIFETIME: self.metrics.peek(
                            (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME),
                            default=0,
                        )
                    },
                )
                self.metrics.aggregate(learner_results, key=LEARNER_RESULTS)

                sub_iter += 1
                self.metrics.log_value(
                    NUM_GRAD_UPDATES_LIFETIME, 1, reduce="lifetime_sum"
                )

        # Log videos showing how the decoder produces observation predictions
        # from the posterior states.
        # Only every n iterations and only for the first sampled batch row
        # (videos are `config.batch_length_T` frames long).
        report_predicted_vs_sampled_obs(
            # TODO (sven): DreamerV3 is single-agent only.
            metrics=self.metrics,
            sample=sample,
            batch_size_B=self.config.batch_size_B,
            batch_length_T=self.config.batch_length_T,
            symlog_obs=do_symlog_obs(
                single_observation_space,
                self.config.symlog_obs,
            ),
            do_report=(
                self.config.report_images_and_videos
                and self.training_iteration % 100 == 0
            ),
        )

        # Log videos showing some of the dreamed trajectories and compare them with the
        # actual trajectories from the train batch.
        # Only every n iterations and only for the first sampled batch row AND first ts.
        # (videos are `config.horizon_H` frames long originating from the observation
        # at B=0 and T=0 in the train batch).
        report_dreamed_eval_trajectory_vs_samples(
            metrics=self.metrics,
            sample=sample,
            burn_in_T=0,
            dreamed_T=self.config.horizon_H + 1,
            dreamer_model=self.env_runner.module.dreamer_model,
            symlog_obs=do_symlog_obs(
                single_observation_space,
                self.config.symlog_obs,
            ),
            do_report=(
                self.config.report_dream_data and self.training_iteration % 100 == 0
            ),
            framework=self.config.framework_str,
        )

        # Update weights - after learning on the LearnerGroup - on all EnvRunner
        # workers.
        with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
            # Only necessary if RLModule is not shared between (local) EnvRunner and
            # (local) Learner.
            # if not self.config.share_module_between_env_runner_and_learner:
            self.metrics.log_value(NUM_SYNCH_WORKER_WEIGHTS, 1, reduce="sum")
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )

        # Add train results and the actual training ratio to stats. The latter should
        # be close to the configured `training_ratio`.
        self.metrics.log_value("actual_training_ratio", self.training_ratio, window=1)

    @property
    def training_ratio(self) -> float:
        """Returns the actual training ratio of this Algorithm (not the configured one).

        The training ratio is copmuted by dividing the total number of steps
        trained thus far (replayed from the buffer) over the total number of actual
        env steps taken thus far.
        """
        eps = 0.0001
        return self.metrics.peek(NUM_ENV_STEPS_TRAINED_LIFETIME, default=0) / (
            (
                self.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME),
                    default=eps,
                )
                or eps
            )
        )

    # TODO (sven): Remove this once DreamerV3 is on the new SingleAgentEnvRunner.
    @PublicAPI
    def __setstate__(self, state) -> None:
        """Sts the algorithm to the provided state

        Args:
            state: The state dictionary to restore this `DreamerV3` instance to.
                `state` may have been returned by a call to an `Algorithm`'s
                `__getstate__()` method.
        """
        # Call the `Algorithm`'s `__setstate__()` method.
        super().__setstate__(state=state)

        # Assign the module to the local `EnvRunner` if sharing is enabled.
        # Note, in `Learner.restore_from_path()` the module is first deleted
        # and then a new one is built - therefore the worker has no
        # longer a copy of the learner.
        if self.config.share_module_between_env_runner_and_learner:
            assert id(self.env_runner.module) != id(
                self.learner_group._learner.module[DEFAULT_MODULE_ID]
            )
            self.env_runner.module = self.learner_group._learner.module[
                DEFAULT_MODULE_ID
            ]
