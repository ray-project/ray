"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
import dataclasses
import gc
import logging
import tree  # pip install dm_tree
from typing import Any, Dict, List, Optional

import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dreamerv3.dreamerv3_catalog import DreamerV3Catalog
from ray.rllib.algorithms.dreamerv3.dreamerv3_learner import (
    DreamerV3LearnerHyperparameters,
)
from ray.rllib.algorithms.dreamerv3.utils import do_symlog_obs
from ray.rllib.algorithms.dreamerv3.utils.env_runner import DreamerV3EnvRunner
from ray.rllib.algorithms.dreamerv3.utils.summaries import (
    summarize_predicted_vs_sampled_obs,
    summarize_sampling_and_replay_buffer,
)
from ray.rllib.core.learner.learner import LearnerHyperparameters
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import one_hot
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    GARBAGE_COLLECTION_TIMER,
    LEARN_ON_BATCH_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_GRAD_UPDATES_LIFETIME,
    NUM_SYNCH_WORKER_WEIGHTS,
    NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.typing import ResultDict


logger = logging.getLogger(__name__)

_, tf, _ = try_import_tf()


class DreamerV3Config(AlgorithmConfig):
    """Defines a configuration class from which a DreamerV3 can be built.

    Example:
        >>> from ray.rllib.algorithms.dreamerv3 import DreamerV3Config
        >>> config = DreamerV3Config()
        >>> config = config.training(  # doctest: +SKIP
        ...     batch_size_B=8, model_size="M"
        ... )
        >>> config = config.resources(num_gpus=4)  # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.dreamerv3 import DreamerV3Config
        >>> from ray import air
        >>> from ray import tune
        >>> config = DreamerV3Config()
        >>> # Print out some default values.
        >>> print(config.training_ratio)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training(   # doctest: +SKIP
        ...     training_ratio=tune.grid_search([256, 512, 1024])
        ... )
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1")  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "DreamerV3",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
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

        self.lr = None
        self.world_model_lr = 1e-4
        self.actor_lr = 3e-5
        self.critic_lr = 3e-5
        self.batch_size_B = 16
        self.batch_length_T = 64
        self.burn_in_T = 5
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

        # Reporting.
        self.summarize_individual_batch_item_stats = False
        self.summarize_dream_data = False
        self.summarize_images_and_videos = False
        self.gc_frequency_train_steps = 100

        # Override some of AlgorithmConfig's default values with DreamerV3-specific
        # values.
        self.rollout_fragment_length = 1
        self.gamma = 0.997  # [1] eq. 7.
        # Do not use! Use `batch_size_B` and `batch_length_T` instead.
        self.train_batch_size = None
        self.env_runner_cls = DreamerV3EnvRunner
        self.num_rollout_workers = 0
        # Dreamer only runs on the new API stack.
        self._enable_learner_api = True
        self._enable_rl_module_api = True
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        model_size: Optional[str] = NotProvided,
        training_ratio: Optional[float] = NotProvided,
        gc_frequency_train_steps: Optional[int] = NotProvided,
        batch_size_B: Optional[int] = NotProvided,
        batch_length_T: Optional[int] = NotProvided,
        burn_in_T: Optional[int] = NotProvided,
        horizon_H: Optional[int] = NotProvided,
        gae_lambda: Optional[float] = NotProvided,
        entropy_scale: Optional[float] = NotProvided,
        return_normalization_decay: Optional[float] = NotProvided,
        train_critic: Optional[bool] = NotProvided,
        train_actor: Optional[bool] = NotProvided,
        intrinsic_rewards_scale: Optional[float] = NotProvided,
        world_model_grad_clip_by_global_norm: Optional[float] = NotProvided,
        critic_grad_clip_by_global_norm: Optional[float] = NotProvided,
        actor_grad_clip_by_global_norm: Optional[float] = NotProvided,
        **kwargs,
    ) -> "DreamerV3Config":
        """Sets the training related configuration.

        Args:
            model_size: The main switch (given as a string such as "S", "M", or
                "L") for adjusting the overall model size. See [1] (table B) for more
                information. Individual model settings, such as the sizes of individual
                layers can still be overwritten by the user.
            training_ratio: The ratio of replayed steps (used for learning/updating the
                model) over env steps (from the actual environment, not the dreamed
                one).
            gc_frequency_train_steps: Every how many training steps do we collect
                garbage?
            batch_size_B: The batch size (B) interpreted as number of rows (each of
                length `batch_length_T`) to sample from the replay buffer in each
                iteration.
            batch_length_T: The batch length (T) interpreted as the length of each row
                sampled from the replay buffer in each iteration. Note that
                `batch_size_B` rows will be sampled in each iteration. Rows normally
                contain consecutive data (consecutive timesteps from the same episode),
                but there might be episode boundaries in a row as well.
            burn_in_T: The number of timesteps we use to "initialize" (burn-in) an
                (evaluation-only!) dream_trajectory run. For this many timesteps,
                the posterior (actual observation data) will be used to compute z,
                after that, only the prior (dynamics network) will be used
                (to compute z^). Note that this setting is NOT used for training.
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
            world_model_grad_clip_by_global_norm: World model grad clipping value
                (by global norm).
            critic_grad_clip_by_global_norm: Critic grad clipping value
                (by global norm).
            actor_grad_clip_by_global_norm: Actor grad clipping value (by global norm).

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if model_size is not NotProvided:
            self.model_size = model_size
        if training_ratio is not NotProvided:
            self.training_ratio = training_ratio
        if gc_frequency_train_steps is not NotProvided:
            self.gc_frequency_train_steps = gc_frequency_train_steps
        if batch_size_B is not NotProvided:
            self.batch_size_B = batch_size_B
        if batch_length_T is not NotProvided:
            self.batch_length_T = batch_length_T
        if burn_in_T is not NotProvided:
            self.burn_in_T = burn_in_T
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
        if world_model_grad_clip_by_global_norm is not NotProvided:
            self.world_model_grad_clip_by_global_norm = (
                world_model_grad_clip_by_global_norm
            )
        if critic_grad_clip_by_global_norm is not NotProvided:
            self.critic_grad_clip_by_global_norm = critic_grad_clip_by_global_norm
        if actor_grad_clip_by_global_norm is not NotProvided:
            self.actor_grad_clip_by_global_norm = actor_grad_clip_by_global_norm

        return self

    @override(AlgorithmConfig)
    def reporting(
        self,
        *,
        summarize_individual_batch_item_stats: Optional[bool] = NotProvided,
        summarize_dream_data: Optional[bool] = NotProvided,
        summarize_images_and_videos: Optional[bool] = NotProvided,
        **kwargs,
    ):
        """Sets the reporting related configuration.

        Args:
            summarize_individual_batch_item_stats: Whether to summarize also individual
                batch items' loss and other stats. If True, will not only provide the
                mean values, e.g. CRITIC_L_total, but also the individual values on
                both batch and time axes, e.g. CRITIC_L_total_B_T.
            summarize_dream_data:  Whether to summarize the dreamed trajectory data.
                If True, will still slice the entire dream data down to the shape
                (H, B, t=0, ...), where H is the horizon and B is the batch size. The
                original time axis will only be represented by the first timestep
                to not make this data too large to handle.
            summarize_images_and_videos: Whether to summarize any image/video data.
            **kwargs:

        Returns:
            This updated AlgorithmConfig object.
        """
        super().reporting(**kwargs)

        if summarize_individual_batch_item_stats is not NotProvided:
            self.summarize_individual_batch_item_stats = (
                summarize_individual_batch_item_stats
            )
        if summarize_dream_data is not NotProvided:
            self.summarize_dream_data = summarize_dream_data
        if summarize_images_and_videos is not NotProvided:
            self.summarize_images_and_videos = summarize_images_and_videos

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call the super class' validation method first.
        super().validate()

        if not (self.burn_in_T + self.horizon_H <= self.batch_length_T):
            raise ValueError(
                f"`burn_in_T` ({self.burn_in_T}) + horizon_H ({self.horizon_H}) must "
                f"be <= batch_length_T ({self.batch_length_T})!"
            )
        # Cannot train actor w/o critic.
        if self.train_actor and not self.train_critic:
            raise ValueError(
                "Cannot train actor network (`train_actor=True`) w/o training critic! "
                "Make sure you either set `train_critic=True` or `train_actor=False`."
            )
        # Use DreamerV3 specific batch size settings.
        if self.train_batch_size is not None:
            raise ValueError(
                "`train_batch_size` should NOT be set! Use `batch_size_B` and "
                "`batch_length_T` instead."
            )
        # Must be run with `EpisodeReplayBuffer` type.
        if self.replay_buffer_config.get("type") != "EpisodeReplayBuffer":
            raise ValueError(
                "DreamerV3 must be run with the `EpisodeReplayBuffer` type! None "
                "other supported."
            )

    @override(AlgorithmConfig)
    def get_learner_hyperparameters(self) -> LearnerHyperparameters:
        base_hps = super().get_learner_hyperparameters()
        return DreamerV3LearnerHyperparameters(
            model_size=self.model_size,
            training_ratio=self.training_ratio,
            batch_size_B=self.batch_size_B,
            batch_length_T=self.batch_length_T,
            horizon_H=self.horizon_H,
            gamma=self.gamma,
            gae_lambda=self.gae_lambda,
            entropy_scale=self.entropy_scale,
            return_normalization_decay=self.return_normalization_decay,
            train_actor=self.train_actor,
            train_critic=self.train_critic,
            world_model_lr=self.world_model_lr,
            intrinsic_rewards_scale=self.intrinsic_rewards_scale,
            actor_lr=self.actor_lr,
            critic_lr=self.critic_lr,
            world_model_grad_clip_by_global_norm=(
                self.world_model_grad_clip_by_global_norm
            ),
            actor_grad_clip_by_global_norm=self.actor_grad_clip_by_global_norm,
            critic_grad_clip_by_global_norm=self.critic_grad_clip_by_global_norm,
            summarize_individual_batch_item_stats=(
                self.summarize_individual_batch_item_stats
            ),
            summarize_dream_data=self.summarize_dream_data,
            summarize_images_and_videos=self.summarize_images_and_videos,
            **dataclasses.asdict(base_hps),
        )

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "tf2":
            from ray.rllib.algorithms.dreamerv3.tf.dreamerv3_tf_learner import (
                DreamerV3TfLearner,
            )

            return DreamerV3TfLearner
        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> SingleAgentRLModuleSpec:
        if self.framework_str == "tf2":
            from ray.rllib.algorithms.dreamerv3.tf.dreamerv3_tf_rl_module import (
                DreamerV3TfRLModule,
            )

            return SingleAgentRLModuleSpec(
                module_class=DreamerV3TfRLModule, catalog_class=DreamerV3Catalog
            )
        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")


class DreamerV3(Algorithm):
    """Implementation of the model-based DreamerV3 RL algorithm described in [1]."""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return DreamerV3Config()

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        # Summarize (single-agent) RLModule once here.
        # TODO: use local worker instead of learner group's local Learner
        if self.config.framework_str == "tf2":
            self.learner_group._learner.module[DEFAULT_POLICY_ID].dreamer_model.summary(
                expand_nested=True
            )

        # The vectorized gymnasium EnvRunner to collect samples of shape (B, T, ...).
        # self.env_runner = EnvRunner(model=None, config=self.config)
        # env_runner_evaluation = EnvRunnerV2(model=None, config=self.config)

        # Create a replay buffer for storing actual env samples.
        self.replay_buffer = EpisodeReplayBuffer(
            capacity=self.config.replay_buffer_config["capacity"],
            batch_size_B=self.config.batch_size_B,
            batch_length_T=self.config.batch_length_T,
        )

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        results = {}

        env_runner = self.workers.local_worker()

        # Push enough samples into buffer initially before we start training.
        env_steps = 0
        if self.training_iteration == 0:
            logger.info(
                "Filling replay buffer so it contains at least "
                f"{self.config.batch_size_B * self.config.batch_length_T} timesteps "
                "(required for a single train batch)."
            )

        # Sample one round and place collected data into our replay buffer.
        # If the buffer is empty at the beginning, sample for as long as it contains
        # enough data for at least one complete train batch
        # (batch_size_B x batch_length_T), only then proceeed to the training
        # update step.
        with self._timers[SAMPLE_TIMER]:
            while True:
                done_episodes, ongoing_episodes = env_runner.sample()

                # We took B x T env steps.
                env_steps_last_sample = sum(
                    len(eps) for eps in done_episodes + ongoing_episodes
                )
                env_steps += env_steps_last_sample
                self._counters[NUM_AGENT_STEPS_SAMPLED] += env_steps_last_sample
                self._counters[NUM_ENV_STEPS_SAMPLED] += env_steps_last_sample

                # Add ongoing and finished episodes into buffer. The buffer will
                # automatically take care of properly concatenating (by episode IDs)
                # the different chunks of the same episodes, even if they come in via
                # separate `add()` calls.
                self.replay_buffer.add(episodes=done_episodes + ongoing_episodes)

                ts_in_buffer = self.replay_buffer.get_num_timesteps()
                if (
                    # More timesteps than BxT.
                    ts_in_buffer
                    >= (self.config.batch_size_B * self.config.batch_length_T)
                    # And enough timesteps for the next train batch to not exceed
                    # the training_ratio.
                    and self._counters[NUM_ENV_STEPS_TRAINED]
                    / self._counters[NUM_ENV_STEPS_SAMPLED]
                    < self.config.training_ratio
                ):
                    # Summarize environment interaction and buffer data.
                    results[ALL_MODULES] = summarize_sampling_and_replay_buffer(
                        replay_buffer=self.replay_buffer,
                    )
                    break

        # Repeat the following steps until we reach the correct training ratio:
        # - Draw a sample from the replay buffer and push it through the
        # LearnerGroup.update() procedure.
        # - This will perform a dreamer model forward pass, compute all losses and
        # gradients and apply the gradients to the models.
        replayed_steps = sub_iter = 0
        while replayed_steps / env_steps_last_sample < self.config.training_ratio:

            # Time individual batch updates.
            with self._timers[LEARN_ON_BATCH_TIMER]:
                logger.info(f"\tSub-iteration {self.training_iteration}/{sub_iter})")

                # Draw a new sample from the replay buffer.
                sample = self.replay_buffer.sample(
                    batch_size_B=self.config.batch_size_B,
                    batch_length_T=self.config.batch_length_T,
                )
                replayed_steps += self.config.batch_size_B * self.config.batch_length_T

                # Convert some bool columns to float32 and one-hot actions.
                sample["is_first"] = sample["is_first"].astype(np.float32)
                sample["is_last"] = sample["is_last"].astype(np.float32)
                sample["is_terminated"] = sample["is_terminated"].astype(np.float32)
                if isinstance(env_runner.env.single_action_space, gym.spaces.Discrete):
                    sample["actions_ints"] = sample[SampleBatch.ACTIONS]
                    sample[SampleBatch.ACTIONS] = one_hot(
                        sample["actions_ints"],
                        depth=env_runner.env.single_action_space.n,
                    )

                # Perform the actual update via our learner group.
                train_results = self.learner_group.update(
                    SampleBatch(sample).as_multi_agent(),
                    reduce_fn=self._reduce_results,
                )

                self._counters[NUM_AGENT_STEPS_TRAINED] += replayed_steps
                self._counters[NUM_ENV_STEPS_TRAINED] += replayed_steps

                # Perform additional (non-gradient updates), such as the critic EMA-copy
                # update.
                with self._timers["critic_ema_update"]:
                    self.learner_group.additional_update(
                        timestep=self._counters[NUM_ENV_STEPS_TRAINED],
                        reduce_fn=self._reduce_results,
                    )

                if self.config.summarize_images_and_videos:
                    summarize_predicted_vs_sampled_obs(
                        # TODO (sven): DreamerV3 is single-agent only.
                        results=train_results[DEFAULT_POLICY_ID],
                        sample=sample,
                        batch_size_B=self.config.batch_size_B,
                        batch_length_T=self.config.batch_length_T,
                        symlog_obs=do_symlog_obs(
                            env_runner.env.single_observation_space,
                            self.config.model.get("symlog_obs", "auto"),
                        ),
                    )

                # TODO: Make this work with any renderable env.
                # if env_runner.config.env in [
                #    "CartPoleDebug-v0",
                #    "CartPole-v1",
                #    "FrozenLake-v1",
                # ]:
                #    summarize_dreamed_trajectory(
                #        # TODO (sven): DreamerV3 is single-agent only.
                #        results=train_results[DEFAULT_POLICY_ID],
                #        env=env_runner.config.env,
                #        dreamer_model=env_runner.rl_module.dreamer_model,
                #        obs_dims_shape=sample[SampleBatch.OBS].shape[2:],
                #        desc="for_actor_critic_learning",
                #        include_images=self.config.summarize_images_and_videos,
                #    )

                res = train_results[DEFAULT_POLICY_ID]
                logger.info(
                    f"\t\tWORLD_MODEL_L_total={res['WORLD_MODEL_L_total']:.5f} ("
                    f"L_pred={res['WORLD_MODEL_L_prediction']:.5f} ("
                    f"decoder/obs={res['WORLD_MODEL_L_decoder']} "
                    f"L_rew={res['WORLD_MODEL_L_reward']} "
                    f"L_cont={res['WORLD_MODEL_L_continue']}); "
                    f"L_dyn/rep={res['WORLD_MODEL_L_dynamics']:.5f})"
                )
                msg = "\t\t"
                if self.config.train_actor:
                    msg += f"L_actor={res['ACTOR_L_total']:.5f} "
                if self.config.train_critic:
                    msg += f"L_critic={res['CRITIC_L_total']:.5f} "
                logger.info(msg)

                sub_iter += 1
                self._counters[NUM_GRAD_UPDATES_LIFETIME] += 1

        # Update weights - after learning on the LearnerGroup - on all EnvRunner
        # workers.
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS] = 0
            self._counters[NUM_SYNCH_WORKER_WEIGHTS] += 1
            self.workers.sync_weights(from_worker_or_trainer=self.learner_group)

        # Try trick from https://medium.com/dive-into-ml-ai/dealing-with-memory-leak-
        # issue-in-keras-model-training-e703907a6501
        if self.config.gc_frequency_train_steps and (
            self._counters[NUM_GRAD_UPDATES_LIFETIME]
            % self.config.gc_frequency_train_steps
            == 0
        ):
            with self._timers[GARBAGE_COLLECTION_TIMER]:
                gc.collect()

        results.update(train_results)

        # Return all results.
        return results

    @staticmethod
    def _reduce_results(results: List[Dict[str, Any]]):
        return tree.map_structure(lambda *s: np.mean(s, axis=0), *results)
