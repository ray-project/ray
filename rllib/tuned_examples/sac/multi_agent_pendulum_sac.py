from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.tune.registry import register_env

from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args()
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env(
    "multi_agent_pendulum",
    lambda _: MultiAgentPendulum({"num_agents": args.num_agents or 2}),
)

config = (
    SACConfig()
    .environment(env="multi_agent_pendulum")
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "relu",
            "post_fcnet_hiddens": [],
            "post_fcnet_activation": None,
            "post_fcnet_weights_initializer": "orthogonal_",
            "post_fcnet_weights_initializer_config": {"gain": 0.01},
        }
    )
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .env_runners(
        rollout_fragment_length=1,
        num_env_runners=2,
        num_envs_per_env_runner=1,
    )
    .training(
        initial_alpha=1.001,
        lr=3e-4,
        target_entropy="auto",
        n_step=1,
        tau=0.005,
        train_batch_size_per_learner=256,
        target_network_update_freq=1,
        replay_buffer_config={
            "type": "MultiAgentEpisodeReplayBuffer",
            "capacity": 100000,
        },
        num_steps_sampled_before_learning_starts=256,
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
    # TODO (simon): If using only a single agent this leads to errors.
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={"p0", "p1"},
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 500000,
    # `episode_return_mean` is the sum of all agents/policies' returns.
    "env_runner_results/episode_return_mean": -400.0 * (args.num_agents or 1),
}

if __name__ == "__main__":
    # from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    # run_rllib_example_script_experiment(config, args, stop=stop)

    from ray import train, tune
    import ray

    ray.init(local_mode=True)
    tuner = tune.Tuner(
        "SAC",
        param_space=config,
        run_config=train.RunConfig(
            stop=stop,
        ),
    )
    tuner.fit()
