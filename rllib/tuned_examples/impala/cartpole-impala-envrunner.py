from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray import air, tune


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .environment("CartPole-v1")
    .rollouts(env_runner_cls=SingleAgentEnvRunner)
    .resources(
        num_learner_workers=0,
        num_gpus_per_learner_worker=0,
        num_gpus=0,
    )
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
    )
)

stop = {
    "timesteps_total": 50000000,
    "sampler_results/episode_reward_mean": 150.0,
}


if __name__ == "__main__":
    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()
