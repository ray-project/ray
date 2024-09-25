from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.random_env import RandomLargeObsSpaceEnv


config = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    # Switch off np.random, which is known to have memory leaks.
    .environment(RandomLargeObsSpaceEnv, env_config={"static_samples": True})
    .env_runners(
        num_env_runners=4,
        num_envs_per_env_runner=5,
    )
    .training(train_batch_size=500, minibatch_size=256, num_epochs=5)
)
