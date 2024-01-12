from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


config = (
    PPOConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=4,
    )
    .environment("CartPole-v1")
    .training(
        gamma=0.99,
        lr=0.0003,
        num_sgd_iter=6,
        vf_loss_coeff=0.01,
        model={
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
    "evaluation/sampler_results/episode_reward_mean": 400.0,
}


if __name__ == "__main__":
    from ray import air, tune

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()
