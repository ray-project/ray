from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.tune.registry import register_env


register_env("multi_agent_pendulum", lambda _: MultiAgentPendulum({"num_agents": 2}))

config = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("multi_agent_pendulum")
    .env_runners(
        num_envs_per_env_runner=1,
        num_env_runners=2,
    )
    .rl_module(
        model_config_dict={
            "fcnet_activation": "relu",
            "uses_new_env_runners": True,
        },
    )
    .training(
        train_batch_size=512,
        lambda_=0.1,
        gamma=0.95,
        lr=0.0003,
        sgd_minibatch_size=64,
        vf_clip_param=10.0,
    )
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={"p0", "p1"},
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 500000,
    # Divide by num_agents for actual reward per agent.
    "env_runner_results/episode_return_mean": -800.0,
}


if __name__ == "__main__":
    from ray import air, tune

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop, verbose=2),
    )
    results = tuner.fit()
