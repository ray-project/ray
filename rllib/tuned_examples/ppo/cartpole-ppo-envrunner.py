from ray.rllib.algorithms.ppo import PPOConfig

# TODO (sven): Gives circular import error.
# from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


def return_env_runner_cls(cls: str = "SingleAgentEnvRunner"):
    if cls == "SingleAgentEnvRunner":
        from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

        return SingleAgentEnvRunner
    else:
        from ray.rllib.evaluation.rollout_worker import RolloutWorker

        return RolloutWorker


config = (
    PPOConfig()
    .environment("CartPole-v1")
    .framework(framework="tf2", eager_tracing=True)
    .rollouts(
        num_rollout_workers=1,
        env_runner_cls=return_env_runner_cls(),
        # TODO (simon): Add the "MeanStd' filtering
        # when available in the EnvRunner stack.
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_num_workers=0,
        enable_async_evaluation=True,
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
    .debugging(seed=0)
)

stop = {
    "timesteps_total": 100000,
    "evaluation/sampler_results/episode_reward_mean": 150.0,
}
