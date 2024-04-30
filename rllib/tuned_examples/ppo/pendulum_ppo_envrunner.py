from ray.rllib.algorithms.ppo import PPOConfig


config = (
    PPOConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .env_runners(
        num_env_runners=2,
        num_envs_per_env_runner=20,
    )
    .environment("Pendulum-v1")
    .training(
        train_batch_size_per_learner=512,
        gamma=0.95,
        lr=0.0003,
        lambda_=0.1,
        vf_clip_param=10.0,
        sgd_minibatch_size=64,
        model={
            "fcnet_activation": "relu",
            "uses_new_env_runners": True,
        },
    )
    .evaluation(
        evaluation_num_env_runners=1,
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 400000,
    "evaluation_results/env_runner_results/episode_return_mean": -400.0,
}
