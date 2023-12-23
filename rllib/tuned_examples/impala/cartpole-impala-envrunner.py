from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .environment("CartPole-v1")
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=2,
    )
    .resources(num_learner_workers=1)
    .training(
        grad_clip=40.0,
        grad_clip_by="global_norm",
    )
    #.evaluation(
    #    evaluation_num_workers=1,
    #    evaluation_interval=1,
    #    enable_async_evaluation=True,
    #)
)

stop = {
    "timesteps_total": 500000,
    "sampler_results/episode_reward_mean": 150.0,
}


if __name__ == "__main__":
    import ray
    ray.init()

    algo = config.build_algorithm()
    for _ in range(1000):
        results = algo.train()
        print(results)
        print(f"R={results['sampler_results']['episode_reward_mean']}")
