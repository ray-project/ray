from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.algorithms import Algorithm
from ray.rllib.offline.json_writer import JsonWriter
import numpy as np
import os
import shutil


def collect_data(
    algo: Algorithm,
    checkpoint_dir: str,
    name: str,
    stop_reward: float,
    num_episodes: int,
):
    mean_ret = []
    gamma = algo.config["gamma"]
    results = algo.train()
    while results["episode_reward_mean"] < stop_reward:
        results = algo.train()

    checkpoint = algo.save_checkpoint(checkpoint_dir)
    checkpoint_path = os.path.join(checkpoint_dir, "checkpoint", name)
    os.renames(checkpoint, checkpoint_path)

    output_path = os.path.join(checkpoint_dir, "data", name)
    writer = JsonWriter(output_path)
    n_episodes = 0
    while n_episodes < num_episodes:
        episodes = synchronous_parallel_sample(worker_set=algo.workers, concat=False)
        for episode in episodes:
            ep_ret = 0
            for r in episode[SampleBatch.REWARDS][::-1]:
                ep_ret = r + gamma * ep_ret
            mean_ret.append(ep_ret)
            writer.write(episode)
            n_episodes += 1
    return checkpoint_path, output_path, np.mean(mean_ret), np.std(mean_ret)


config = (
    DQNConfig()
    .environment(env="CartPole-v0")
    .framework("torch")
    .rollouts(num_rollout_workers=8, batch_mode="complete_episodes")
)
algo = config.build()
checkpoint_dir = "/tmp/cartpole/"
shutil.rmtree(checkpoint_dir, ignore_errors=True)
os.makedirs(checkpoint_dir, exist_ok=True)
num_episodes = 100

print(collect_data(algo, checkpoint_dir, "random", 20, num_episodes))
print(collect_data(algo, checkpoint_dir, "mixed", 120, num_episodes))
print(collect_data(algo, checkpoint_dir, "optimal", 200, num_episodes))
