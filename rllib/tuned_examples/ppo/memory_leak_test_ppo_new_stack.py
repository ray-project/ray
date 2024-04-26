from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.random_env import RandomLargeObsSpaceEnv


config = (
    PPOConfig()
    .experimental(_enable_new_api_stack=True)
    # Switch off np.random, which is known to have memory leaks.
    .environment(RandomLargeObsSpaceEnv, env_config={"static_samples": True})
    .env_runners(
        env_runner_cls=SingleAgentEnvRunner,
        num_env_runners=4,
        num_envs_per_env_runner=5,
    )
    .training(train_batch_size=500, sgd_minibatch_size=256, num_sgd_iter=5)
)
