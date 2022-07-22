from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn import DQNConfig
import os
import shutil

checkpoint_dir = "/tmp/cartpole/torch/"
shutil.rmtree(checkpoint_dir, ignore_errors=True)
os.makedirs(checkpoint_dir, exist_ok=True)
config = (
    DQNConfig()
    .environment(env="CartPole-v0")
    .framework("torch")
    .rollouts(num_rollout_workers=4)
)
algo = config.build()
random = algo.save_checkpoint(checkpoint_dir)
results = algo.train()

while results["episode_reward_max"] < 200:
    results = algo.train()
mixed = algo.save_checkpoint(checkpoint_dir)

while results["episode_reward_mean"] < 200:
    results = algo.train()
optimal = algo.save_checkpoint(checkpoint_dir)
algo.stop()


def generate_data(
    config: AlgorithmConfig, checkpoint=None, output_path=None, num_episodes=1000
):
    # Uses Algorithm.evaluate() to generate output data
    config.evaluation(
        evaluation_duration=num_episodes,
        evaluation_interval=None,
        evaluation_duration_unit="episodes",
        evaluation_num_workers=4,
    ).rollouts(num_rollout_workers=0)
    if output_path:
        config.evaluation(evaluation_config={"output": output_path})
    algo = config.build()
    if checkpoint:
        algo.load_checkpoint(checkpoint)
    algo.evaluate()
    algo.stop()

generate_data(config, output_path=checkpoint_dir + "data/random")
generate_data(config, mixed, output_path=checkpoint_dir + "data/mixed")
generate_data(config, optimal, output_path=checkpoint_dir + "data/optimal")