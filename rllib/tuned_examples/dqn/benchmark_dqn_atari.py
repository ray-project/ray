from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.tune import Stopper
from ray import train, tune

# Needs the following packages to be installed on Ubuntu:
#   sudo apt-get libosmesa-dev
#   sudo apt-get install patchelf
#   python -m pip install "gymnasium[mujoco]"
# Might need to be added to bashsrc:
#   export MUJOCO_GL=osmesa"
#   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/.mujoco/mujoco200/bin"

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


# Define a `tune.Stopper` that stops the training if the benchmark is reached
# or the maximum number of timesteps is exceeded.
class BenchmarkStopper(Stopper):
    def __init__(self, benchmark_envs):
        self.benchmark_envs = benchmark_envs

    def __call__(self, trial_id, result):
        # Stop training if the mean reward is reached.
        if (
            result["sampler_results/episode_reward_mean"]
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


config = (
    DQNConfig()
    .environment(env=tune.grid_search(list(benchmark_envs.keys())))
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        rollout_fragment_length=1,
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=0,
    )
    # TODO (simon): Adjust to new model_config_dict.
    .training(
        initial_alpha=1.001,
        lr=3e-4,
        target_entropy="auto",
        n_step=1,
        tau=0.005,
        train_batch_size=256,
        target_network_update_freq=1,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 1000000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        num_steps_sampled_before_learning_starts=256,
        model={
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "relu",
            "post_fcnet_hiddens": [],
            "post_fcnet_activation": None,
            "post_fcnet_weights_initializer": "orthogonal_",
            "post_fcnet_weights_initializer_config": {"gain": 0.01},
        },
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
    .evaluation(
        evaluation_duration="auto",
        evaluation_interval=1,
        evaluation_num_workers=1,
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
        name="benchmark_sac_mujoco",
    ),
)

tuner.fit()
