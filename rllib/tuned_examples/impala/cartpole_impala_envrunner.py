from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray import air, tune


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .environment("CartPole-v1")
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=2,
        # TODO (sven): Add MeanStd connector, once fully tested (should learn much
        #  better with it).
    )
    .resources(
        num_learner_workers=2,
        num_cpus_per_learner_worker=1,
        num_gpus_per_learner_worker=0,
        num_gpus=0,
        num_cpus_for_local_worker=1,
    )
    .training(
        #train_batch_size_per_learner=500,
        #grad_clip=40.0,
        #grad_clip_by="global_norm",
        #lr=[[0, 0.0005], [2000000, 0.000005]],
        model={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
        }
    )
)

stop = {
    "timesteps_total": 50000000,
    "sampler_results/episode_reward_mean": 200.0,
}


if __name__ == "__main__":
    import ray
    ray.init(num_cpus=6)

    algo = config.build_algorithm()
    for _ in range(1000):
        results = algo.train()
        print(results)#f"R={results['episode_reward_mean']}")
        #print(f"sampled={algo._counters['num_env_steps_sampled']} trained={algo._counters['num_env_steps_trained']} learner_group_ts_dropped={algo._counters['learner_group_ts_dropped']} actor_manager_num_outstanding_async_reqs={algo._counters['actor_manager_num_outstanding_async_reqs']}")
    algo.stop()
    quit()

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=2,
        ),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()
