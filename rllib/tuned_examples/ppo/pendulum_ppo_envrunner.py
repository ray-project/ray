from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray import air, tune


config = (
    PPOConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=1,
        # TODO (sven): Add MeanStd connector, once fully tested (should learn much
        #  better with it).
        #observation_filter="MeanStdFilter",
        env_to_module_connector=lambda env: MeanStdFilter(
            input_observation_space=env.single_observation_space,
            input_action_space=env.single_action_space,
        ),
    )
    .environment("Pendulum-v1")
    .training(
        train_batch_size_per_learner=512,
        gamma=0.95,
        lr=0.0001,
        lambda_=0.1,
        vf_loss_coeff=0.01,
        vf_clip_param=10.0,
        mini_batch_size_per_learner=64,
        #model={
        #    "fcnet_activation": "relu",
        #},
    )
    #.evaluation(
    #    evaluation_num_workers=1,
    #    evaluation_interval=1,
    #    enable_async_evaluation=True,
    #)
)

stop = {
    "timesteps_total": 400000,
    "sampler_results/episode_reward_mean": -400.0,
}


if __name__ == "__main__":
    #import ray
    #ray.init()#TODO

    #algo = config.build_algorithm()
    #for _ in range(1000):
    #    results = algo.train()
    #    print(f"R={results['episode_reward_mean']}")
    #    #print(f"sampled={algo._counters['num_env_steps_sampled']} trained={algo._counters['num_env_steps_trained']} learner_group_ts_dropped={algo._counters['learner_group_ts_dropped']} actor_manager_num_outstanding_async_reqs={algo._counters['actor_manager_num_outstanding_async_reqs']}")
    #algo.stop()
    #quit()

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=1,
        ),
        tune_config=tune.TuneConfig(num_samples=5),
    )
    results = tuner.fit()

