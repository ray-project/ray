import gymnasium as gym
from pathlib import Path

from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.offline.offline_evaluation_runner_group import (
    OfflineEvaluationRunnerGroup,
)

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
        num_offline_eval_runners=2,
    )
    .learners(
        num_learners=0,
    )
    .training(
        train_batch_size_per_learner=256,
    )
)

algo = config.build()

module_state = algo.learner_group._learner.module.get_state()
# import ray
# ray.init(local_mode=True)
offline_runner_group = OfflineEvaluationRunnerGroup(
    config=config,
    local_runner=False,
    module_state=module_state,
)

metrics = offline_runner_group.foreach_runner(
    "run",
    kwargs=[{"num_samples": 1} for _ in range(offline_runner_group.num_remote_runners)],
)

print(metrics)
