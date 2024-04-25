from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    PPOConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=2,
        num_envs_per_worker=20,
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
        evaluation_num_workers=1,
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
    )
)

stop = {
    "num_env_steps_sampled_lifetime": 400000,
    "evaluation_results/env_runner_results/episode_return_mean": -400.0,
}
