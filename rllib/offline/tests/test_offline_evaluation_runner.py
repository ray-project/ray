import gymnasium as gym
from pathlib import Path

from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.offline.offline_evaluation_runner import OfflineEvaluationRunner

data_path = "tests/data/cartpole/cartpole-v1_large"
base_path = Path(__file__).parents[2]
data_path = "local://" + base_path.joinpath(data_path).as_posix()
# Assign the observation and action spaces.
env = gym.make("CartPole-v1")
observation_space = env.observation_space
action_space = env.action_space

# Create a simple config.
config = (
    BCConfig()
    .environment(
        observation_space=observation_space,
        action_space=action_space,
    )
    .api_stack(
        enable_env_runner_and_connector_v2=True,
        enable_rl_module_and_learner=True,
    )
    .offline_data(
        input_=[data_path],
        dataset_num_iters_per_learner=1,
    )
    .learners(
        num_learners=0,
    )
    .training(
        train_batch_size_per_learner=256,
    )
)

# Create an algorithm from the config.
algo = config.build()

iterators = algo.offline_data.sample(
    num_samples=256,
    return_iterator=True,
    num_shards=0,
)

offline_eval_runner = OfflineEvaluationRunner(config=config)

offline_eval_runner.set_dataset_iterator(iterators[0])

metrics = offline_eval_runner.run(num_samples=1)

print(metrics)
