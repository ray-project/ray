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
        num_learner_workers=1,
        num_gpus=0,
        num_cpus_for_local_worker=1,
    )
    .training(
        #train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        #lr=[[0, 0.0005], [2000000, 0.000005]],
        vf_loss_coeff=0.1,
        #entropy_coeff=[[0, 0.02], [2000000, 0.005]],
        model={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
            "uses_new_env_runners": True,
        }
    )
)

stop = {
    "timesteps_total": 20000000,
    "sampler_results/episode_reward_mean": 400.0,
}


if __name__ == "__main__":
    #import ray
    #ray.init(num_cpus=6)

    #algo = config.build_algorithm()
    #for _ in range(1000):
    #    results = algo.train()
    #    print(results)#f"R={results['episode_reward_mean']}")
    #    #print(f"sampled={algo._counters['num_env_steps_sampled']} trained={algo._counters['num_env_steps_trained']} learner_group_ts_dropped={algo._counters['learner_group_ts_dropped']} actor_manager_num_outstanding_async_reqs={algo._counters['actor_manager_num_outstanding_async_reqs']}")
    #algo.stop()
    #quit()

    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment, add_rllib_example_script_args

    parser = add_rllib_example_script_args()
    args = parser.parse_args()

    run_rllib_example_script_experiment(config, args, stop)
