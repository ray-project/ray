from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray import tune

tune.registry.register_env("env", lambda cfg: MultiAgentCartPole(config=cfg))


config = (
    ImpalaConfig()
    .environment("env", env_config={"num_agents": 4})
    .rollouts(
        num_envs_per_worker=5,
        num_rollout_workers=4,
        observation_filter="MeanStdFilter",
    )
    .resources(num_gpus=1, _fake_gpus=True)
    .multi_agent(
        policies=["p0", "p1", "p2", "p3"],
        policy_mapping_fn=(lambda agent_id, episode, worker, **kwargs: f"p{agent_id}"),
    )
    .training(
        num_sgd_iter=1,
        vf_loss_coeff=0.005,
        vtrace=True,
        model={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
        },
        replay_proportion=0.0,
    )
)

stop = {
    "sampler_results/episode_reward_mean": 600,  # 600 / 4 (==num_agents) = 150
    "timesteps_total": 200000,
}
