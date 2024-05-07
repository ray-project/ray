from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.tune.registry import register_env

from ray import train, tune

register_env("multi_agent_pendulum", lambda _: MultiAgentPendulum({"num_agents": 2}))

config = (
    SACConfig()
    .api_stack(
        enable_env_runner_and_connector_v2=True,
        enable_rl_module_and_learner=True,
    )
    .env_runners(
        rollout_fragment_length=1,
        num_env_runners=2,
        num_envs_per_env_runner=1,
    )
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
    .training(
        initial_alpha=1.001,
        lr=3e-4,
        target_entropy="auto",
        n_step=1,
        tau=0.005,
        train_batch_size=256,
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
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={"p0", "p1"},
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 500000,
    # divide by num_agents for actual reward per agent.
    "env_runner_results/episode_return_mean": -800.0,
}

if __name__ == "__main__":

    # TODO (simon): Use test_utils for this example
    # and add to BUILD learning tests.
    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=train.RunConfig(stop=stop, verbose=2),
    )
    results = tuner.fit()
