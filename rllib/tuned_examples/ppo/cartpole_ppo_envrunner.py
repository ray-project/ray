from ray.rllib.algorithms.ppo import PPOConfig


config = (
    PPOConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .env_runners(num_env_runners=1)
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
        use_kl_loss=True,
    )
    .evaluation(
        evaluation_num_env_runners=1,
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 100000,
    "evaluation_results/env_runner_results/episode_return_mean": 150.0,
}
