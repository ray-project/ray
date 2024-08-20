import pathlib

from ray.rllib.algorithms.ppo import PPOConfig

# Configure and train an old stack default ModelV2.
config = (
    PPOConfig()
    .api_stack(
        enable_env_runner_and_connector_v2=False,
        enable_rl_module_and_learner=False,
    )
    .environment("CartPole-v1")
)
algo = config.build()

min_return = 200.0
while True:
    results = algo.train()
    print(results)
    if results["env_runners"]["episode_return_mean"] >= min_return:
        print(
            f"Reached episode return of {min_return} -> stopping old API stack "
            "training."
        )
        break

checkpoint = algo.save()
policy_path = pathlib.Path(checkpoint.checkpoint.path) / "policies" / "default_policy"
assert policy_path.is_dir()

print("done")

import gymnasium as gym
import numpy as np
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.examples.rl_modules.classes.modelv2_by_policy_checkpoint_rlm import (
    ModelV2ByPolicyCheckpointRLModule
)
from ray.rllib.utils.spaces.space_utils import batch

# Run a simple CartPole inference experiment.
env = gym.make("CartPole-v1")
rl_module = ModelV2ByPolicyCheckpointRLModule(
    config=RLModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config_dict={"policy_checkpoint_dir": policy_path},
    ),
)

obs, _ = env.reset()
done = False
while not done:
    action_logits = rl_module.forward_inference({"obs": batch([obs])})
    action = np.argmax(action_logits[0])
