"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
import logging
from typing import Optional, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.typing import ResultDict


logger = logging.getLogger(__name__)


class DreamerV3Config(AlgorithmConfig):
    """Defines a configuration class from which a DreamerV3 can be built.

    Example:
        >>> from ray.rllib.algorithms.dreamerv3 import DreamerV3Config
        >>> config = DreamerV3Config()
        >>> config = config.training(  # doctest: +SKIP
        ...     batch_size_B=8, model_dimension="M"
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
        self.model_dimension = "XS"
        self.training_ratio = 1024

        self.replay_buffer_config = {
            "type": "EpisodeReplayBuffer",
            "capacity": int(1e6),
        }

        #self.num_pretrain_iterations = 0
        self.summary_frequency_train_steps = 20
        self.summary_include_histograms = False
        self.gc_frequency_train_steps = 100
        self.batch_size_B = 16
        self.batch_length_T = 64
        self.burn_in_T = 5
        self.horizon_H = 15
        self.gae_lambda = 0.95  # [1] eq. 7.
        self.entropy_scale = 3e-4  # [1] eq. 11.
        self.return_normalization_decay = 0.99  # [1] eq. 11 and 12.
        self.symlog_obs = "auto"
        self.train_critic = True
        self.train_actor = True
        self.use_curiosity = False
        self.intrinsic_rewards_scale = 0.1
        self.world_model_grad_clip_by_global_norm = 1000.0
        self.critic_grad_clip_by_global_norm = 100.0
        self.actor_grad_clip_by_global_norm = 100.0
        self.disagree_grad_clip_by_global_norm = 100.0

        # Override some of PG/AlgorithmConfig's default values with PPO-specific values.
        self.rollout_fragment_length = 1
        self.gamma = 0.997  # [1] eq. 7.
        # Do not use! Use `batch_size_B` and `batch_length_T` instead.
        self.train_batch_size = None

        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        model_dimension: Optional[str] = NotProvided,
        training_ratio: Optional[float] = NotProvided,
        summary_frequency_train_steps: Optional[int] = NotProvided,
        summary_include_histograms: Optional[bool] = NotProvided,
        gc_frequency_train_steps: Optional[int] = NotProvided,
        batch_size_B: Optional[int] = NotProvided,
        batch_length_T: Optional[int] = NotProvided,
        burn_in_T: Optional[int] = NotProvided,
        horizon_H: Optional[int] = NotProvided,
        gae_lambda: Optional[float] = NotProvided,
        entropy_scale: Optional[float] = NotProvided,
        return_normalization_decay: Optional[float] = NotProvided,
        symlog_obs: Optional[Union[bool, str]] = NotProvided,
        train_critic: Optional[bool] = NotProvided,
        train_actor: Optional[bool] = NotProvided,
        use_curiosity: Optional[bool] = NotProvided,
        intrinsic_rewards_scale: Optional[float] = NotProvided,
        world_model_grad_clip_by_global_norm: Optional[float] = NotProvided,
        critic_grad_clip_by_global_norm: Optional[float] = NotProvided,
        actor_grad_clip_by_global_norm: Optional[float] = NotProvided,
        disagree_grad_clip_by_global_norm: Optional[float] = NotProvided,
        **kwargs,
    ) -> "DreamerV3Config":
        """Sets the training related configuration.

        Args:
            model_dimension: The main switch (given as a string such as "S", "M", or
                "L") for adjusting the overall model size. See [1] (table B) for more
                information. Individual model settings, such as the sizes of individual
                layers can still be overwritten by the user.
            training_ratio: The ratio of replayed steps (used for learning/updating the
                model) over env steps (from the actual environment, not the dreamed
                one).
            #num_pretrain_iterations: How many iterations do we pre-train?
            summary_frequency_train_steps: Every how many training steps do we write
                summary data (e.g. TensorBoard or WandB)? Note that this only affects
                the more debug-relevant information and basic stats, such as total loss,
                etc.. are logged at each training step.
            summary_include_histograms: Whether to summarize histograms data as well.
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
            symlog_obs: Whether to symlog the observations or not. If "auto", will
                symlog only non-image observations.
            train_critic: Whether to train the critic network. If False, `train_actor`
                must also be False (cannot train actor w/o training the critic).
            train_actor: Whether to train the actor network. If True, `train_critic`
                must also be True (cannot train actor w/o training the critic).
            use_curiosity: Whether to use the disagree-networks to compute intrinsic
                rewards for the dreamed data that critic and actor learn from.
            intrinsic_rewards_scale: The factor to multiply intrinsic rewards with
                before adding them to the extrinsic (environment) rewards.
            world_model_grad_clip_by_global_norm: World model grad clipping value
                (by global norm).
            critic_grad_clip_by_global_norm: Critic grad clipping value
                (by global norm).
            actor_grad_clip_by_global_norm: Actor grad clipping value (by global norm).
            disagree_grad_clip_by_global_norm: Disagree net (curiosity) grad clipping
                value (by global norm).

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if model_dimension is not NotProvided:
            self.model_dimension = model_dimension
        if training_ratio is not NotProvided:
            self.training_ratio = training_ratio
        if summary_frequency_train_steps is not NotProvided:
            self.summary_frequency_train_steps = summary_frequency_train_steps
        if summary_include_histograms is not NotProvided:
            self.summary_include_histograms = summary_include_histograms
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
        if symlog_obs is not NotProvided:
            self.symlog_obs = symlog_obs
        if train_critic is not NotProvided:
            self.train_critic = train_critic
        if train_actor is not NotProvided:
            self.train_actor = train_actor
        if use_curiosity is not NotProvided:
            self.use_curiosity = use_curiosity
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
        if disagree_grad_clip_by_global_norm is not NotProvided:
            self.disagree_grad_clip_by_global_norm = disagree_grad_clip_by_global_norm

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

    def is_image_space(self) -> bool:
        """Returns whether we have an image observation space or not."""
        return len(self.observation_space.shape) in [2, 3]

    def do_symlog_obs(self) -> bool:
        """Returns whether to symlog the observations or not."""

        # If our symlog_obs setting is NOT set specifically ("auto"), return True
        # if we don't have an image observation space, otherwise False.
        # TODO (sven): Fix for mixed observations.
        return (
            not self.is_image_space() if self.symlog_obs == "auto" else self.symlog_obs
        )

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "tf2":
            from ray.rllib.algorithms.dreamerv3.tf.dreamerv3_tf_learner import (
                DreamerV3TfLearner
            )

            return DreamerV3TfLearner
        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> SingleAgentRLModuleSpec:
        if self.framework_str == "tf2":
            from ray.rllib.algorithms.dreamerv3.tf.dreamerv3_tf_rl_module import (
                DreamerV3TfRLModule
            )

            return SingleAgentRLModuleSpec(
                module_class=DreamerV3TfRLModule, catalog_class=DreamerV3Catalog
            )
        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")


class DreamerV3(Algorithm):
    """
    """

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return DreamerV3Config()

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)
        # The vectorized gymnasium EnvRunner to collect samples of shape (B, T, ...).
        env_runner = EnvRunnerV2(model=None, config=algo_config)
        env_runner_evaluation = EnvRunnerV2(model=None, config=algo_config)

        # Create a replay buffer for storing actual env samples.
        self.buffer = EpisodeReplayBuffer(
            capacity=self.config.replay_buffer_config["capacity"],
            batch_size_B=self.config.batch_size_B,
            batch_length_T=self.config.batch_length_T,
        )

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        # Push enough samples into buffer initially before we start training.
        env_steps = env_steps_last_sample = 0
        # TEST: Put only a single row in the buffer and try to memorize it.
        # env_steps_last_sample = 64
        # while iteration == 0:
        # END TEST

        if iteration == training_iteration_start:
            print(
                "Filling replay buffer so it contains at least "
                f"{self.config.batch_size_B * self.config.batch_length_T} ts "
                "(required for a single train batch)."
            )

        while True:
            # Sample one round.
            done_episodes, ongoing_episodes = self.env_runner.sample(random_actions=False)

            # We took B x T env steps.
            env_steps_last_sample = sum(
                len(eps) for eps in done_episodes + ongoing_episodes
            )
            env_steps += env_steps_last_sample
            total_env_steps += env_steps_last_sample

            # Add ongoing and finished episodes into buffer. The buffer will
            # automatically take care of properly concatenating (by episode IDs) the
            # different chunks of the same episodes, even if they come in via separate
            # `add()` calls.
            self.buffer.add(episodes=done_episodes + ongoing_episodes)

            ts_in_buffer = self.buffer.get_num_timesteps()
            if (
                # Got to have more timesteps than warm up setting.
                ts_in_buffer > warm_up_timesteps
                # More timesteps than BxT.
                and ts_in_buffer >= self.config.batch_size_B * self.config.batch_length_T
                # And enough timesteps for the next train batch to not exceed
                # the training_ratio.
                and total_replayed_steps / total_env_steps < training_ratio
                ## But also at least as many episodes as the batch size B.
                ## Actually: This is not useful for longer episode envs, such as Atari.
                ## Too much initial data goes into the buffer, then.
                # and episodes_in_buffer >= batch_size_B
            ):
                # Summarize environment interaction and buffer data.
                summarize_sampling_and_replay_buffer(
                    step=total_env_steps,
                    replay_buffer=self.buffer,
                    sampler_metrics=env_runner.get_metrics(),
                    print_=True,
                )
                break

        replayed_steps = 0

        # TEST: re-use same sample.
        # sample = buffer.sample(num_items=batch_size_B)
        # sample = tree.map_structure(lambda v: tf.convert_to_tensor(v), sample)
        # END TEST

        sub_iter = 0
        while replayed_steps / env_steps_last_sample < self.config.training_ratio:
            print(f"\tSub-iteration {iteration}/{sub_iter})")

            # Draw a new sample from the replay buffer.
            sample = self.buffer.sample(
                batch_size_B=self.config.batch_size_B,
                batch_length_T=self.config.batch_length_T,
            )
            replayed_steps += self.config.batch_size_B * self.config.batch_length_T

            # Convert samples (numpy) to tensors.
            sample = tree.map_structure(lambda v: tf.convert_to_tensor(v), sample)
            # Do some other conversions.
            sample["is_first"] = tf.cast(sample["is_first"], tf.float32)
            sample["is_last"] = tf.cast(sample["is_last"], tf.float32)
            sample["is_terminated"] = tf.cast(sample["is_terminated"], tf.float32)
            if isinstance(action_space, gym.spaces.Discrete):
                sample["actions_ints"] = sample["actions"]
                sample["actions"] = tf.one_hot(
                    sample["actions_ints"], depth=action_space.n
                )

            # Perform the actual update via our learner group.
            train_results = self.learner_group.update(sample)

            if summary_frequency_train_steps and (
                total_train_steps % summary_frequency_train_steps == 0
            ):
                summarize_forward_train_outs_vs_samples(
                    forward_train_outs=world_model_forward_train_outs,
                    sample=sample,
                    batch_size_B=self.config.batch_size_B,
                    batch_length_T=self.config.batch_length_T,
                    symlog_obs=self.config.do_symlog_obs(),
                )
                summarize_world_model_train_results(
                    world_model_train_results=world_model_train_results,
                    include_histograms=self.config.summary_include_histograms,
                )
                # Summarize actor-critic loss stats.
                if self.config.train_critic:
                    summarize_critic_train_results(
                        actor_critic_train_results=actor_critic_train_results,
                        include_histograms=self.config.summary_include_histograms,
                    )
                if self.config.train_actor:
                    summarize_actor_train_results(
                        actor_critic_train_results=actor_critic_train_results,
                        include_histograms=self.config.summary_include_histograms,
                    )
                if self.config.use_curiosity:
                    summarize_disagree_train_results(
                        actor_critic_train_results=actor_critic_train_results,
                        include_histograms=self.config.summary_include_histograms,
                    )
                # TODO: Make this work with any renderable env.
                if self.env_runner.config.env in [
                    "CartPoleDebug-v0", "CartPole-v1", "FrozenLake-v1"
                ]:
                    summarize_dreamed_trajectory(
                        dream_data=actor_critic_train_results["dream_data"],
                        actor_critic_train_results=actor_critic_train_results,
                        env=env_runner.config.env,
                        dreamer_model=dreamer_model,
                        obs_dims_shape=sample["obs"].shape[2:],
                        desc="for_actor_critic_learning",
                    )

            logger.info(
                "\t\tWORLD_MODEL_L_total="
                f"{world_model_train_results['WORLD_MODEL_L_total'].numpy():.5f} ("
                "L_pred="
                f"{world_model_train_results['WORLD_MODEL_L_prediction'].numpy():.5f} ("
                f"dec/obs={world_model_train_results['WORLD_MODEL_L_decoder'].numpy()} "
                f"rew(two-hot)={world_model_train_results['WORLD_MODEL_L_reward'].numpy()} "
                f"cont={world_model_train_results['WORLD_MODEL_L_continue'].numpy()}"
                "); "
                f"L_dyn={world_model_train_results['WORLD_MODEL_L_dynamics'].numpy():.5f}; "
                "L_rep="
                f"{world_model_train_results['WORLD_MODEL_L_representation'].numpy():.5f})"
            )
            msg = "\t\t"
            if self.config.train_actor:
                L_actor = actor_critic_train_results["ACTOR_L_total"]
                msg += f"L_actor={L_actor.numpy() if self.config.train_actor else 0.0:.5f} "
            if self.config.train_critic:
                L_critic = actor_critic_train_results["CRITIC_L_total"]
                msg += f"L_critic={L_critic.numpy():.5f} "
            if self.config.use_curiosity:
                L_disagree = actor_critic_train_results["DISAGREE_L_total"]
                msg += f"L_disagree={L_disagree.numpy():.5f}"
            logger.info(msg)

            sub_iter += 1
            total_train_steps += 1

        total_replayed_steps += replayed_steps

        # Try trick from https://medium.com/dive-into-ml-ai/dealing-with-memory-leak-
        # issue-in-keras-model-training-e703907a6501
        if self.config.gc_frequency_train_steps and (
            total_train_steps % self.config.gc_frequency_train_steps == 0
        ):
            gc.collect()


# Our DreamerV3 world model.
print(f"Creating initial DreamerModel ...")
model_dimension = config["model_dimension"]
gray_scaled = is_img_space and len(env_runner.env.single_observation_space.shape) == 2
world_model = WorldModel(
    model_dimension=model_dimension,
    action_space=action_space,
    batch_length_T=batch_length_T,
    num_gru_units=config.get("num_gru_units"),
    encoder=(
        CNNAtari(model_dimension=model_dimension) if is_img_space
        else MLP(model_dimension=model_dimension)
    ),
    decoder=ConvTransposeAtari(
        model_dimension=model_dimension,
        gray_scaled=gray_scaled,
    ) if is_img_space else VectorDecoder(
        model_dimension=model_dimension,
        observation_space=env_runner.env.single_observation_space,
    ),
    symlog_obs=symlog_obs,
)
dreamer_model = DreamerModel(
    model_dimension=model_dimension,
    action_space=action_space,
    world_model=world_model,
    use_curiosity=use_curiosity,
    intrinsic_rewards_scale=intrinsic_rewards_scale,
    batch_size_B=batch_size_B,
    batch_length_T=batch_length_T,
    horizon_H=horizon_H,
)

# Timesteps to put into the buffer before the first learning step.
warm_up_timesteps = 0

# Load state from a saved checkpoint.
if args.checkpoint is not None:
    # dreamer_model = tf.keras.models.load_model(args.checkpoint)
    # Load buffer.
    print("LOADING data from checkpoint into buffer.")
    buffer.set_state(
        np.load(f"{args.checkpoint}/buffer.npz", allow_pickle=True)["state"])

    total_env_steps = int(np.load(f"{args.checkpoint}/total_env_steps.npy"))
    total_replayed_steps = int(np.load(f"{args.checkpoint}/total_replayed_steps.npy"))
    total_train_steps = int(np.load(f"{args.checkpoint}/total_train_steps.npy"))
    training_iteration_start = int(np.load(f"{args.checkpoint}/iteration.npy"))
else:
    total_env_steps = 0
    total_replayed_steps = 0
    total_train_steps = 0
    training_iteration_start = 0

# TODO: ugly hack (resulting from the insane fact that you cannot know
#  an env's spaces prior to actually constructing an instance of it) :(
env_runner.model = dreamer_model
env_runner_evaluation.model = dreamer_model
