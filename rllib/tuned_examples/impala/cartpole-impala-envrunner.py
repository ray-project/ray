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
    .resources(
        num_learner_workers=4,
        num_gpus_per_learner_worker=0,
        # num_gpus=4,
        # _fake_gpus=True,
    )
    .training(
        # train_batch_size=2000,
        train_batch_size_per_learner=500,
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
    "timesteps_total": 50000000,
    "sampler_results/episode_reward_mean": 150.0,
}


if __name__ == "__main__":
    import ray
    ray.init()

    from ray import air, tune

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()

    #algo = config.build_algorithm()
    #for _ in range(1000):
    #    results = algo.train()
    #    print(results)
    #    print(f"R={results['sampler_results']['episode_reward_mean']}")
