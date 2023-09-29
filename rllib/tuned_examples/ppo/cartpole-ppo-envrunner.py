from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.utils.ppo_env_runner import PPOEnvRunner

config = (
    PPOConfig()
    .environment("CartPole-v1")
    .framework(framework="tf2")
    .rollouts(
        num_rollout_workers=1,
        env_runner_cls=PPOEnvRunner,
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_num_workers=1,
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
)

stop = {
    "timesteps_total": 100000,
    "evaluation/episode_reward_mean": 150.0,
}
