# __rllib-first-config-begin__
from ray.rllib.algorithms.ppo import PPOConfig
from ray.tune.logger import pretty_print


algo = (
    PPOConfig()
    .rollouts(num_rollout_workers=1)
    .resources(num_gpus=0)
    .environment(env="CartPole-v1")
    .build()
)

for i in range(1000):
    result = algo.train()
    print(pretty_print(result))

    if i % 100 == 0:
        checkpoint_dir = algo.save()
        print(f"Checkpoint saved in directory {checkpoint_dir}")
# __rllib-first-config-end__


# __rllib-tune-config-begin__
import ray
from ray import air, tune


ray.init()

config = {
    "env": "CartPole-v1",
    "num_gpus": 0,
    "num_workers": 1,
    "lr": tune.grid_search([0.01, 0.001, 0.0001]),
}

tuner = tune.Tuner(
    "PPO",
    run_config=air.RunConfig(
        stop={"episode_reward_mean": 200},
    ),
    param_space=config,
)

tuner.fit()
# __rllib-tune-config-end__

# __rllib-tuner-begin__
# ``Tuner.fit()`` allows setting a custom log directory (other than ``~/ray-results``)
results = ray.tune.Tuner(
    "PPO",
    param_space=config,
    run_config=air.RunConfig(
        stop={"episode_reward_mean": 200},
        checkpoint_config=air.CheckpointConfig(checkpoint_at_end=True),
    ),
).fit()

# list of lists: one list per checkpoint; each checkpoint list contains
# 1st the path, 2nd the metric value
checkpoints = results.get_trial_checkpoints_paths(
    trial=results.get_best_trial("episode_reward_mean"), metric="episode_reward_mean"
)

# or simply get the last checkpoint (with highest "training_step")
last_checkpoint = results.get_last_checkpoint()
# if there are multiple trials, select a specific trial or automatically
# choose the best one according to a given metric
last_checkpoint = results.get_last_checkpoint(metric="episode_reward_mean", mode="max")
# __rllib-tuner-end__


# __rllib-compute-action-begin__
import gym
from ray.rllib.algorithms.ppo import PPOConfig

env_name = "CartPole-v1"
algo = PPOConfig().environment(env=env_name).build()
env = gym.make(env_name)

episode_reward = 0
done = False
obs = env.reset()
while not done:
    action = algo.compute_single_action(obs)
    obs, reward, done, info = env.step(action)
    episode_reward += reward
# __rllib-compute-action-end__

# __rllib-get-state-begin__
from ray.rllib.algorithms.dqn import DQNConfig

algo = DQNConfig().environment(env="CartPole-v1").build()

# Get weights of the default local policy
algo.get_policy().get_weights()

# Same as above
algo.workers.local_worker().policy_map["default_policy"].get_weights()

# Get list of weights of each worker, including remote replicas
algo.workers.foreach_worker(lambda worker: worker.get_policy().get_weights())

# Same as above, but with index.
algo.workers.foreach_worker_with_index(
    lambda worker, _: worker.get_policy().get_weights()
)
# __rllib-get-state-end__
