import gymnasium as gym
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.connectors.env_to_module import (
    AddObservationsFromEpisodesToBatch,
    FlattenObservations,
    WriteObservationsToEpisodes,
)
from ray.rllib.env.multi_agent_env import make_multi_agent

from ray.tune.registry import register_env
from ray import train, tune

from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args()
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

MultiAgentFrozenLake = make_multi_agent(
    lambda config: gym.make("FrozenLake-v1", desc=["SH", "FG"], is_slippery=False)
)
register_env(
    "multi_agent_frozenlake", lambda _: MultiAgentFrozenLake({"num_agents": 2})
)

config = (
    DQNConfig()
    .environment(
        env="multi_agent_frozenlake",
    )
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [256],
            "fcnet_activation": "relu",
            "epsilon": [(0, 1.0), (10000, 0.02)],
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
            "uses_new_env_runners": True,
        },
    )
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .env_runners(
        rollout_fragment_length=1,
        num_env_runners=2,
        num_envs_per_env_runner=1,
        env_to_module_connector=lambda env: [
            AddObservationsFromEpisodesToBatch(),
            FlattenObservations(multi_agent=True),
            WriteObservationsToEpisodes(),
        ],
    )
    .training(
        double_q=True,
        num_atoms=1,
        noisy=False,
        dueling=True,
        replay_buffer_config={
            "type": "MultiAgentPrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        num_steps_sampled_before_learning_starts=256,
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={"p0", "p1"},
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 500000,
    # `episode_return_mean` is the sum of all agents/policies' returns.
    "env_runner_results/episode_return_mean": 2.0,  # * (args.num_agents or 1),
}

if __name__ == "__main__":
    # from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    # run_rllib_example_script_experiment(config, args, stop=stop)

    # TODO (simon): Use test_utils for this example
    # and add to BUILD learning tests.
    import ray

    ray.init(local_mode=True)
    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=train.RunConfig(stop=stop, verbose=2),
    )
    results = tuner.fit()
