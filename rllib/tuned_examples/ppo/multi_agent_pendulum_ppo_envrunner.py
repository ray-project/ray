from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.env.multi_agent import MultiAgentPendulum
from ray.tune.registry import register_env


register_env("multi_agent_pendulum", lambda _: MultiAgentPendulum({"num_agents": 2}))

config = (
    PPOConfig()
    .experimental(_enable_new_api_stack=True)
    .environment("multi_agent_pendulum")
    .rollouts(
        env_runner_cls=MultiAgentEnvRunner,
        num_envs_per_worker=1,
        num_rollout_workers=4,
    )
    .training(
        train_batch_size=512,
        lambda_=0.1,
        gamma=0.95,
        lr=0.0003,
        sgd_minibatch_size=64,
        model={
            "fcnet_activation": "relu",
            "uses_new_env_runners": True,
        },
        vf_clip_param=10.0,
    )
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={"p0", "p1"},
    )
)

stop = {
    "timesteps_total": 500000,
    "episode_reward_mean": -1000.0,  # agents required to reach -500 on average
}


if __name__ == "__main__":
    from ray import air, tune

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop, verbose=2),
    )
    results = tuner.fit()
