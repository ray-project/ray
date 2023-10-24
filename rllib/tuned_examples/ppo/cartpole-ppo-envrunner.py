from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    PPOConfig()
    .environment("CartPole-v1")
    .rollouts(
        num_rollout_workers=1,
        env_runner_cls=SingleAgentEnvRunner,
    )
    .training(
        gamma=0.99,
        lr=0.0003,
        num_sgd_iter=6,
        vf_loss_coeff=0.01,
        model={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
        },
    )
    .evaluation(
        evaluation_num_workers=1,
        evaluation_interval=1,
        enable_async_evaluation=True,
    )
)

stop = {
    "timesteps_total": 100000,
    "evaluation/sampler_results/episode_reward_mean": 150.0,
}
