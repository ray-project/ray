import tree  # pip install dm_tree

from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
)


class VPGConfig(AlgorithmConfig):
    """A simple VPG (vanilla policy gradient) algorithm w/o value function support.

    Use for testing purposes only!

    This Algorithm should use the VPGTorchLearner and VPGTorchRLModule
    """

    # A test setting to activate metrics on mean weights.
    report_mean_weights: bool = True

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or VPG)

        # VPG specific settings.
        self.num_episodes_per_train_batch = 10
        # Note that we don't have to set this here, because we tell the EnvRunners
        # explicitly to sample entire episodes. However, for good measure, we change
        # this setting here either way.
        self.batch_mode = "complete_episodes"

        # VPG specific defaults (from AlgorithmConfig).
        self.num_env_runners = 1

    @override(AlgorithmConfig)
    def training(
        self, *, num_episodes_per_train_batch=NotProvided, **kwargs
    ) -> "VPGConfig":
        """Sets the training related configuration.

        Args:
            num_episodes_per_train_batch: The number of complete episodes per train
                batch. VPG requires entire episodes to be sampled from the EnvRunners.
                For environments with varying episode lengths, this leads to varying
                batch sizes (in timesteps) as well possibly causing slight learning
                instabilities. However, for simplicity reasons, we stick to collecting
                always exactly n episodes per training update.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if num_episodes_per_train_batch is not NotProvided:
            self.num_episodes_per_train_batch = num_episodes_per_train_batch

        return self

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self):
        if self.framework_str == "torch":
            from ray.rllib.examples.rl_modules.classes.vpg_torch_rlm import (
                VPGTorchRLModule,
            )

            spec = RLModuleSpec(
                module_class=VPGTorchRLModule,
                model_config={"hidden_dim": 64},
            )
        else:
            raise ValueError(f"Unsupported framework: {self.framework_str}")

        return spec

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "torch":
            from ray.rllib.examples.learners.classes.vpg_torch_learner import (
                VPGTorchLearner,
            )

            return VPGTorchLearner
        else:
            raise ValueError(f"Unsupported framework: {self.framework_str}")


class VPG(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return VPGConfig()

    @override(Algorithm)
    def training_step(self) -> None:
        """Override of the training_step method of `Algorithm`.

        Runs the following steps per call:
        - Sample B timesteps (B=train batch size). Note that we don't sample complete
        episodes due to simplicity. For an actual VPG algo, due to the loss computation,
        you should always sample only completed episodes.
        - Send the collected episodes to the VPG LearnerGroup for model updating.
        - Sync the weights from LearnerGroup to all EnvRunners.
        """
        # Sample.
        with self.metrics.log_time((TIMERS, ENV_RUNNER_SAMPLING_TIMER)):
            episodes, env_runner_results = self._sample_episodes()
        # Merge results from n parallel sample calls into self's metrics logger.
        self.metrics.merge_and_log_n_dicts(env_runner_results, key=ENV_RUNNER_RESULTS)

        # Just for demonstration purposes, log the number of time steps sampled in this
        # `training_step` round.
        # Mean over a window of 100:
        self.metrics.log_value(
            "episode_timesteps_sampled_mean_win100",
            sum(map(len, episodes)),
            reduce="mean",
            window=100,
        )
        # Exponential Moving Average (EMA) with coeff=0.1:
        self.metrics.log_value(
            "episode_timesteps_sampled_ema",
            sum(map(len, episodes)),
            ema_coeff=0.1,  # <- weight of new value; weight of old avg=1.0-ema_coeff
        )

        # Update model.
        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            learner_results = self.learner_group.update_from_episodes(
                episodes=episodes,
                timesteps={
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                        self.metrics.peek(
                            (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                        )
                    ),
                },
            )
        # Merge results from m parallel update calls into self's metrics logger.
        self.metrics.merge_and_log_n_dicts(learner_results, key=LEARNER_RESULTS)

        # Sync weights.
        with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )

    def _sample_episodes(self):
        # How many episodes to sample from each EnvRunner?
        num_episodes_per_env_runner = self.config.num_episodes_per_train_batch // (
            self.config.num_env_runners or 1
        )
        # Send parallel remote requests to sample and get the metrics.
        sampled_data = self.env_runner_group.foreach_env_runner(
            # Return tuple of [episodes], [metrics] from each EnvRunner.
            lambda env_runner: (
                env_runner.sample(num_episodes=num_episodes_per_env_runner),
                env_runner.get_metrics(),
            ),
            # Loop over remote EnvRunners' `sample()` method in parallel or use the
            # local EnvRunner if there aren't any remote ones.
            local_env_runner=self.env_runner_group.num_remote_workers() <= 0,
        )
        # Return one list of episodes and a list of metrics dicts (one per EnvRunner).
        episodes = tree.flatten([s[0] for s in sampled_data])
        stats_dicts = [s[1] for s in sampled_data]

        return episodes, stats_dicts
