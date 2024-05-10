from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray import tune

tune.registry.register_env("env", lambda cfg: MultiAgentCartPole(config=cfg))


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .environment("env", env_config={"num_agents": 2})
    .rollouts(
        env_runner_cls=MultiAgentEnvRunner,
        env_to_module_connector=lambda env: MeanStdFilter(multi_agent=True),
        num_envs_per_env_runner=1,
        num_rollout_workers=4,
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
        lr=0.0005,
        vf_loss_coeff=0.1,
        model={
            "vf_share_layers": True,
            "uses_new_env_runners": True,
        },
    )
    .multi_agent(
        policies=["p0", "p1"],
        policy_mapping_fn=(lambda agent_id, episode, **kwargs: f"p{agent_id}"),
    )
)

stop = {
    "sampler_results/episode_reward_mean": 800.0,
    "timesteps_total": 400000,
}
