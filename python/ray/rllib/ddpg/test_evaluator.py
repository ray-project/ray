import tensorflow as tf
import numpy as np
import gym
from ddpg import DEFAULT_CONFIG, OPTIMIZER_SHARED_CONFIGS

from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.ddpg.ddpg_evaluator import DDPGEvaluator
from ray.rllib.optimizers.replay_buffer import ReplayBuffer, \
    PrioritizedReplayBuffer
from ray.rllib.optimizers import LocalSyncReplayOptimizer

def main():
    env_creator = lambda env_config: gym.make("Pendulum-v0")
    config = DEFAULT_CONFIG
    config["env"] = "Pendulum-v0"
    config["clip_rewards"] = False
    config["exploration_fraction"] = 0.4

    evaluator = DDPGEvaluator(dict(), env_creator, config, "/temp", 0)
    memory = PrioritizedReplayBuffer(config["buffer_size"], 
                                     config["prioritized_replay_alpha"],
                                     config["clip_rewards"])
    for k in OPTIMIZER_SHARED_CONFIGS:
            if k not in config["optimizer_config"]:
                config["optimizer_config"][k] = config[k]
    print(config["optimizer_config"])
    raw_input()
    optimizer = LocalSyncReplayOptimizer(config["optimizer_config"], evaluator, [])

    last_sync_ts = 0
    start_ts = 0
    num_ts = 0

    while True:
        start_ts = optimizer.num_steps_sampled
        while optimizer.num_steps_sampled - start_ts < config["timesteps_per_iteration"]:
            optimizer.step()
            if optimizer.num_steps_sampled - last_sync_ts > config["target_network_update_freq"]:
                evaluator.update_target()
                last_sync_ts = optimizer.num_steps_sampled

        evaluator.set_global_timestep(optimizer.num_steps_sampled)
        stats = evaluator.stats()
        if not isinstance(stats, list):
            stats = [stats]
        mean_100ep_reward = 0.0
        test_stats = stats
        for s in test_stats:
            mean_100ep_reward += s["mean_100ep_reward"] / len(test_stats)
        print(mean_100ep_reward)

    """
    while True:
        start_ts = num_ts
        while num_ts - start_ts < config["timesteps_per_iteration"]:
            batch = evaluator.sample()
            for row in batch.rows():
                memory.add(
                        row["obs"], row["actions"], row["rewards"], row["new_obs"],
                        row["dones"], row["weights"])
            if len(memory) >= config["learning_starts"]:
                if isinstance(memory, PrioritizedReplayBuffer):
                    (obses_t, actions, rewards, obses_tp1,
                        dones, weights, batch_indexes) = memory.sample(
                            config["train_batch_size"],
                            beta=config["prioritized_replay_beta"])
                else:
                    (obses_t, actions, rewards, obses_tp1,
                        dones) = memory.sample(
                            config["train_batch_size"])
                    weights = np.ones_like(rewards)
                    batch_indexes = - np.ones_like(rewards)

                samples = SampleBatch({
                    "obs": obses_t, "actions": actions, "rewards": rewards,
                    "new_obs": obses_tp1, "dones": dones, "weights": weights,
                    "batch_indexes": batch_indexes})

                td_error = evaluator.compute_apply(samples)["td_error"]
                new_priorities = (
                    np.abs(td_error) + config["prioritized_replay_eps"])
                if isinstance(memory, PrioritizedReplayBuffer):
                    memory.update_priorities(
                        samples["batch_indexes"], new_priorities)

            num_ts += batch.count

            if num_ts - last_sync_ts > config["target_network_update_freq"]:
                evaluator.update_target()
                last_sync_ts = num_ts

        evaluator.set_global_timestep(num_ts)
        stats = evaluator.stats()
        if not isinstance(stats, list):
            stats = [stats]
        mean_100ep_reward = 0.0
        test_stats = stats
        for s in test_stats:
            mean_100ep_reward += s["mean_100ep_reward"] / len(test_stats)
        print(mean_100ep_reward)
        """

if __name__=="__main__":
    main()

