from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    PPOConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=1,
    )
    .environment("CartPole-v1")
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
        }
    )
    .training(
        gamma=0.99,
        lr=0.0003,
        num_sgd_iter=6,
        vf_loss_coeff=0.01,
    )
    .evaluation(
        evaluation_num_workers=1,
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 100000,
    "evaluation_results/env_runner_results/episode_return_mean": 150.0,
}
