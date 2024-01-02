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
        num_rollout_workers=4,
    )
    .resources(
        num_learner_workers=2,
        num_gpus_per_learner_worker=0,
        num_gpus=0,
    )
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
    )
)

stop = {
    "timesteps_total": 50000000,
    "sampler_results/episode_reward_mean": 150.0,
}


if __name__ == "__main__":
    #import ray
    #ray.init()#TODO

    algo = config.build_algorithm()
    for _ in range(1000):
        results = algo.train()
        #print(results)
        print(f"sampled={algo._counters['num_env_steps_sampled']} trained={algo._counters['num_env_steps_trained']} queue_ts_dropped={algo._counters['learner_group_queue_ts_dropped']} queued_ts={algo._counters['learner_group_queue_size']} queue_ts_dropped={algo._counters['learner_group_queue_ts_dropped']} actor_manager_num_outstanding_async_reqs={algo._counters['actor_manager_num_outstanding_async_reqs']}")
    algo.stop()

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()
