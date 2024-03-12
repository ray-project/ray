from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .environment("CartPole-v1")
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=8,
        # TODO (sven): Add MeanStd connector, once fully tested (should learn much
        #  better with it).
    )
    .resources(
        num_learner_workers=1,
        num_gpus=0,
        num_cpus_for_local_worker=1,
    )
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        lr=0.0005,#[[0, 0.0005], [2000000, 0.000005]],
        vf_loss_coeff=0.1,
        #entropy_coeff=1.0,
        #entropy_coeff=[[0, 0.02], [2000000, 0.005]],
        model={
            #"fcnet_hiddens": [32],
            #"fcnet_activation": "linear",
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
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment, add_rllib_example_script_args

    parser = add_rllib_example_script_args()
    args = parser.parse_args()

    run_rllib_example_script_experiment(config, args, stop)
