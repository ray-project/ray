import gymnasium as gym
from gymnasium.wrappers import AtariPreprocessing

from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune import Stopper
from ray import train, tune

# Might need `gymnasium[atari, other]` to be installed.

# See the following links for becnhmark results of other libraries:
#   Original paper: https://arxiv.org/abs/1812.05905
#   CleanRL: https://wandb.ai/cleanrl/cleanrl.benchmark/reports/Mujoco--VmlldzoxODE0NjE
#   AgileRL: https://github.com/AgileRL/AgileRL?tab=readme-ov-file#benchmarks

benchmark_envs = {
    "AlienNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 6022.9,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "AmidarNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 202.8,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "AssaultNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 14491.7,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "AsterixNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 280114.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "AsteroidsNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 2249.4,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "AtlantisNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 814684.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "BankHeistNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 826.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "BattleZoneNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 52040.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "BeamRiderNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 21768.5,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "BerzerkNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 1793.4,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "BowlingNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 39.4,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "BoxingNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 54.9,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "BreakoutNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 379.5,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "CentipedeNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 7160.9,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "ChopperCommandNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 10916.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "CrazyClimberNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 143962.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "DefenderNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 47671.3,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "DemonAttackNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 109670.7,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "DoubleDunkNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -0.6,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "EnduroNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 2061.1,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "FishingDerbyNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 22.6,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "FreewayNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 29.1,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "FrostbiteNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 4141.1,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "GopherNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 72595.7,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "GravitarNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 567.5,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "HeroNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 50496.8,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "IceHockeyNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -11685.8,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "KangarooNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 10841.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "KrullNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 6715.5,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "KungFuMasterNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 28999.8,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "MontezumaRevengeNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 154.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "MsPacmanNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 2570.2,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "NameThisGameNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 11686.5,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "PhoenixNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 103061.6,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "PitfallNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -37.6,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "PongNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 19.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "PrivateEyeNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 1704.4,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "QbertNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 18397.6,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "RoadRunnerNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 54261.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "RobotankNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 55.2,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "SeaquestNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 19176.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "SkiingNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -11685.8,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "SolarisNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 2860.7,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "SpaceInvadersNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 12629.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "StarGunnerNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 123853.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "SurroundNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 7.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "TennisNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -2.2,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "TimePilotNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 11190.5,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "TutankhamNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 126.9,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "VentureNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 45.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "VideoPinballNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 506817.2,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "WizardOfWorNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 14631.5,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "YarsRevengeNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 93007.9,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
    "ZaxxonNoFrameskip-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 19658.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000000,
    },
}

for env in benchmark_envs.keys():
    tune.register_env(
        env,
        lambda ctx, e=env: AtariPreprocessing(
            gym.make(e, **ctx), grayscale_newaxis=True, screen_size=84, noop_max=0
        ),
    )


def _make_env_to_module_connector(env):
    return FrameStackingEnvToModule(
        num_frames=4,
    )


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(
        num_frames=4,
    )


# Define a `tune.Stopper` that stops the training if the benchmark is reached
# or the maximum number of timesteps is exceeded.
class BenchmarkStopper(Stopper):
    def __init__(self, benchmark_envs):
        self.benchmark_envs = benchmark_envs

    def __call__(self, trial_id, result):
        # Stop training if the mean reward is reached.
        if (
            result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
            >= self.benchmark_envs[result["env"]][
                f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
            ]
        ):
            return True
        # Otherwise check, if the total number of timesteps is exceeded.
        elif (
            result[f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}"]
            >= self.benchmark_envs[result["env"]][f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}"]
        ):
            return True
        # Otherwise continue training.
        else:
            return False

    # Note, this needs to implemented b/c the parent class is abstract.
    def stop_all(self):
        return False


# See Table 1 in the Rainbow paper for the hyperparameters.
config = (
    DQNConfig()
    .environment(
        env=tune.grid_search(list(benchmark_envs.keys())),
        env_config={
            "max_episode_steps": 108000,
            "obs_type": "grayscale",
            # The authors actually use an action repetition of 4.
            "repeat_action_probability": 0.25,
        },
        clip_rewards=True,
    )
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .env_runners(
        # Every 4 agent steps a training update is performed.
        rollout_fragment_length=4,
        num_env_runners=1,
        env_to_module_connector=_make_env_to_module_connector,
    )
    # TODO (simon): Adjust to new model_config_dict.
    .training(
        # Note, the paper uses also an Adam epsilon of 0.00015.
        lr=0.0000625,
        n_step=3,
        gamma=0.99,
        tau=1.0,
        train_batch_size=32,
        target_network_update_freq=32000,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 1000000,
            "alpha": 0.5,
            # Note the paper used a linear schedule for beta.
            "beta": 0.4,
        },
        # Note, these are frames.
        num_steps_sampled_before_learning_starts=80000,
        noisy=True,
        num_atoms=51,
        v_min=-10.0,
        v_max=10.0,
        double_q=True,
        dueling=True,
        model={
            "cnn_filter_specifiers": [[32, 8, 4], [64, 4, 2], [64, 3, 1]],
            "fcnet_activation": "tanh",
            "post_fcnet_hiddens": [512],
            "post_fcnet_activation": "relu",
            "post_fcnet_weights_initializer": "orthogonal_",
            "post_fcnet_weights_initializer_config": {"gain": 0.01},
        },
        learner_connector=_make_learner_connector,
    )
    .reporting(
        metrics_num_episodes_for_smoothing=10,
        min_sample_timesteps_per_iteration=1000,
    )
    .evaluation(
        evaluation_duration="auto",
        evaluation_interval=1,
        evaluation_num_env_runners=1,
        evaluation_parallel_to_training=True,
        evaluation_config={
            "explore": False,
        },
    )
)

tuner = tune.Tuner(
    "DQN",
    param_space=config,
    run_config=train.RunConfig(
        stop=BenchmarkStopper(benchmark_envs=benchmark_envs),
        name="benchmark_dqn_atari",
    ),
)

tuner.fit()
