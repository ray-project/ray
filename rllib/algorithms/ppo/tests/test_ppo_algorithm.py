import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_algorithm import PPOAlgorithm
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, EPISODE_RETURN_MEAN


env = gym.make("CartPole-v1")
config = (
    PPOConfig()
    .environment(
        "CartPole-v1",
        observation_space=env.observation_space,
        action_space=env.action_space,
    )
    .debugging(log_level="INFO")
)

algo = PPOAlgorithm(config)

while True:
    results = algo.training_step()
    return_mean = sum(
        res[EPISODE_RETURN_MEAN].peek() for res in results[ENV_RUNNER_RESULTS]
    ) / len(results[ENV_RUNNER_RESULTS])
    print(f"R={return_mean}")

    if return_mean >= 500.0:
        break

print(results)
