from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    PPOConfig()
    .environment("Pendulum-v1")
    .rollouts(
        num_rollout_workers=0,
        num_envs_per_worker=20,
        env_runner_cls=SingleAgentEnvRunner,
    )
    .training(
        train_batch_size=512,
        gamma=0.95,
        lr=0.0003,
        lambda_=0.1,
        vf_loss_coeff=0.01,
        vf_clip_param=10.0,
        sgd_minibatch_size=64,
        model={
            "fcnet_activation": "relu",
        },
    )
)

stop = {
    "timesteps_total": 400000,
    "sampler_results/episode_reward_mean": -400.0,
}
