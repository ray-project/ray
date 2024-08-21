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


# Move the old API stack (trained) ModelV2 into the new API stack's RLModule.
import gymnasium as gym
import numpy as np
import torch

from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.examples.rl_modules.classes.modelv2_by_policy_checkpoint_rlm import (
    ModelV2ByPolicyCheckpointRLModule
)
from ray.rllib.utils.spaces.space_utils import batch

# Run a simple CartPole inference experiment.
env = gym.make("CartPole-v1", render_mode="human")
rl_module = ModelV2ByPolicyCheckpointRLModule(
    config=RLModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config_dict={"policy_checkpoint_dir": policy_path},
    ),
)

obs, _ = env.reset()
env.render()
done = False
episode_return = 0.0
while not done:
    output = rl_module.forward_inference({"obs": torch.from_numpy(batch([obs]))})
    action_logits = output["action_dist_inputs"].detach().numpy()[0]
    action = np.argmax(action_logits)
    obs, reward, terminated, truncated, _ = env.step(action)
    done = terminated or truncated
    episode_return += reward
    env.render()

print(f"Played episode with trained ModelV2 at return={episode_return}")


# Continue training with the (checkpointed) ModelV2.

