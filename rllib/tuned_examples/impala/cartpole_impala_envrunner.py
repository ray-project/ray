from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .environment("CartPole-v1")
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=10,
        #env_to_module_connector=lambda env: MeanStdFilter(),
    )
    .resources(
        num_learner_workers=2,
        num_gpus=0,
        num_cpus_for_local_worker=0,
    )
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        lr=0.0005,
        vf_loss_coeff=0.1,
        model={
            "vf_share_layers": True,
            "uses_new_env_runners": True,
        },
    )
)

stop = {
    "sampler_results/episode_reward_mean": 450.0,
    "timesteps_total": 2000000,
}
