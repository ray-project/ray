import gymnasium as gym

from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.tune import Stopper
from ray import train, tune

# Might need `gymnasium[atari, other]` to be installed.

# See the following links for becnhmark results of other libraries:
#   Original paper: https://arxiv.org/abs/1812.05905
#   CleanRL: https://wandb.ai/cleanrl/cleanrl.benchmark/reports/Mujoco--VmlldzoxODE0NjE
#   AgileRL: https://github.com/AgileRL/AgileRL?tab=readme-ov-file#benchmarks

benchmark_envs = {
    "AlienNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 6022.9,
        "timesteps_total": 200000000,
    },
    "AmidarNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 202.8,
        "timesteps_total": 200000000,
    },
    "AssaultNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 14491.7,
        "timesteps_total": 200000000,
    },
    "AsterixNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 280114.0,
        "timesteps_total": 200000000,
    },
    "AsteroidsNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 2249.4,
        "timesteps_total": 200000000,
    },
    "AtlantisNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 814684.0,
        "timesteps_total": 200000000,
    },
    "BankHeistNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 826.0,
        "timesteps_total": 200000000,
    },
    "BattleZoneNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 52040.0,
        "timesteps_total": 200000000,
    },
    "BeamRiderNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 21768.5,
        "timesteps_total": 200000000,
    },
    "BerzerkNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 1793.4,
        "timesteps_total": 200000000,
    },
    "BowlingNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 39.4,
        "timesteps_total": 200000000,
    },
    "BoxingNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 54.9,
        "timesteps_total": 200000000,
    },
    "BreakoutNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 379.5,
        "timesteps_total": 200000000,
    },
    "CentipedeNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 7160.9,
        "timesteps_total": 200000000,
    },
    "ChopperCommandNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 10916.0,
        "timesteps_total": 200000000,
    },
    "CrazyClimberNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 143962.0,
        "timesteps_total": 200000000,
    },
    "DefenderNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 47671.3,
        "timesteps_total": 200000000,
    },
    "DemonAttackNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 109670.7,
        "timesteps_total": 200000000,
    },
    "DoubleDunkNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": -0.6,
        "timesteps_total": 200000000,
    },
    "EnduroNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 2061.1,
        "timesteps_total": 200000000,
    },
    "FishingDerbyNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 22.6,
        "timesteps_total": 200000000,
    },
    "FreewayNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 29.1,
        "timesteps_total": 200000000,
    },
    "FrostbiteNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 4141.1,
        "timesteps_total": 200000000,
    },
    "GopherNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 72595.7,
        "timesteps_total": 200000000,
    },
    "GravitarNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 567.5,
        "timesteps_total": 200000000,
    },
    "HeroNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 50496.8,
        "timesteps_total": 200000000,
    },
    "IceHockeyNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": -11685.8,
        "timesteps_total": 200000000,
    },
    "KangarooNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 10841.0,
        "timesteps_total": 200000000,
    },
    "KrullNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 6715.5,
        "timesteps_total": 200000000,
    },
    "KungFuMasterNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 28999.8,
        "timesteps_total": 200000000,
    },
    "MontezumaRevengeNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 154.0,
        "timesteps_total": 200000000,
    },
    "MsPacmanNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 2570.2,
        "timesteps_total": 200000000,
    },
    "NameThisGameNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 11686.5,
        "timesteps_total": 200000000,
    },
    "PhoenixNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 103061.6,
        "timesteps_total": 200000000,
    },
    "PitfallNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": -37.6,
        "timesteps_total": 200000000,
    },
    "PongNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 19.0,
        "timesteps_total": 200000000,
    },
    "PrivateEyeNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 1704.4,
        "timesteps_total": 200000000,
    },
    "QbertNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 18397.6,
        "timesteps_total": 200000000,
    },
    "RoadRunnerNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 54261.0,
        "timesteps_total": 200000000,
    },
    "RobotankNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 55.2,
        "timesteps_total": 200000000,
    },
    "SeaquestNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 19176.0,
        "timesteps_total": 200000000,
    },
    "SkiingNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": -11685.8,
        "timesteps_total": 200000000,
    },
    "SolarisNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 2860.7,
        "timesteps_total": 200000000,
    },
    "SpaceInvadersNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 12629.0,
        "timesteps_total": 200000000,
    },
    "StarGunnerNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 123853.0,
        "timesteps_total": 200000000,
    },
    "SurroundNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 7.0,
        "timesteps_total": 200000000,
    },
    "TennisNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": -2.2,
        "timesteps_total": 200000000,
    },
    "TimePilotNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 11190.5,
        "timesteps_total": 200000000,
    },
    "TutankhamNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 126.9,
        "timesteps_total": 200000000,
    },
    "VentureNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 45.0,
        "timesteps_total": 200000000,
    },
    "VideoPinballNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 506817.2,
        "timesteps_total": 200000000,
    },
    "WizardOfWorNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 14631.5,
        "timesteps_total": 200000000,
    },
    "YarsRevengeNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 93007.9,
        "timesteps_total": 200000000,
    },
    "ZaxxonNoFrameskip-v4": {
        "sampler_results/episode_reward_mean": 19658.0,
        "timesteps_total": 200000000,
    },
}

for env in benchmark_envs:
    tune.register_env(
        env,
        # Use the RLlib atari wrapper to squeeze images to 84x84.
        # Note, the default of this wrapper is `framestack=4`.
        lambda ctx: wrap_atari_for_new_api_stack(gym.make(env, **ctx), dim=84),
    )


# Define a `tune.Stopper` that stops the training if the benchmark is reached
# or the maximum number of timesteps is exceeded.
class BenchmarkStopper(Stopper):
    def __init__(self, benchmark_envs):
        self.benchmark_envs = benchmark_envs

    def __call__(self, trial_id, result):
        # Stop training if the mean reward is reached.
        if (
            result["sampler_results"]["episode_reward_mean"]
            >= self.benchmark_envs[result["env"]]["sampler_results/episode_reward_mean"]
        ):
            return True
        # Otherwise check, if the total number of timesteps is exceeded.
        elif (
            result["timesteps_total"]
            >= self.benchmark_envs[result["env"]]["timesteps_total"]
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
            # "sticky actions" but not according to Danijar's 100k configs.
            "repeat_action_probability": 0.0,
            # "full action space" but not according to Danijar's 100k configs.
            "full_action_space": False,
            # Already done by MaxAndSkip wrapper: "action repeat" == 4.
            "frameskip": 1,
            # NOTE, because we use the atari wrapper of RLlib, we also have
            # framestack: 4,
            # dim: 84,
            # NOTE, we do not use grayscale here, so this run will need
            # more memory on GPU and CPU (buffer).
        },
        clip_rewards=True,
    )
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .env_runners(
        # Every 4 agent steps a training update is performed.
        rollout_fragment_length=4,
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=1,
    )
    .resources(
        # We have a train/sample ratio of 1:1 and a batch of 32.
        num_learner_workers=1,
        num_gpus_per_learner_worker=1,
    )
    # TODO (simon): Adjust to new model_config_dict.
    .training(
        # Note, the paper uses also an Adam epsilon of 0.00015.
        lr=0.0000625,
        n_step=1,
        gamma=0.99,
        tau=1.0,
        # TODO (simon): Activate when new model_config_dict is available.
        # epsilon=0.01,
        train_batch_size=32,
        target_network_update_freq=8000,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 1000000,
            "alpha": 0.5,
            # Note the paper used a linear schedule for beta.
            "beta": 0.4,
        },
        # Note, these are frames.
        num_steps_sampled_before_learning_starts=20000,
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
        name="benchmark_dqn_atari_rllib_preprocessing",
    ),
)

tuner.fit()
