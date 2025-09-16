import pathlib

import gymnasium as gym
import numpy as np
import torch

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.rl_modules.classes.modelv2_to_rlm import ModelV2ToRLModule
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.spaces.space_utils import batch

if __name__ == "__main__":
    # Configure and train an old stack default ModelV2.
    config = (
        PPOConfig()
        # Old API stack.
        .api_stack(
            enable_env_runner_and_connector_v2=False,
            enable_rl_module_and_learner=False,
        )
        .environment("CartPole-v1")
        .training(
            lr=0.0003,
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
    )
    algo_old_stack = config.build()

    min_return_old_stack = 100.0
    while True:
        results = algo_old_stack.train()
        print(results)
        if results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= min_return_old_stack:
            print(
                f"Reached episode return of {min_return_old_stack} -> stopping "
                "old API stack training."
            )
            break

    checkpoint = algo_old_stack.save()
    policy_path = (
        pathlib.Path(checkpoint.checkpoint.path) / "policies" / "default_policy"
    )
    assert policy_path.is_dir()
    algo_old_stack.stop()

    print("done")

    # Move the old API stack (trained) ModelV2 into the new API stack's RLModule.
    # Run a simple CartPole inference experiment.
    env = gym.make("CartPole-v1", render_mode="human")
    rl_module = ModelV2ToRLModule(
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config={"policy_checkpoint_dir": policy_path},
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

    print(f"Ran episode with trained ModelV2: return={episode_return}")

    # Continue training with the (checkpointed) ModelV2.

    # We change the original (old API stack) `config` into a new API stack one:
    config = config.api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    ).rl_module(
        rl_module_spec=RLModuleSpec(
            module_class=ModelV2ToRLModule,
            model_config={"policy_checkpoint_dir": policy_path},
        ),
    )

    # Build the new stack algo.
    algo_new_stack = config.build()

    # Train until a higher return.
    min_return_new_stack = 450.0
    passed = False
    for i in range(50):
        results = algo_new_stack.train()
        print(results)
        # Make sure that the model's weights from the old API stack training
        # were properly transferred to the new API RLModule wrapper. Thus, even
        # after only one iteration of new stack training, we already expect the
        # return to be higher than it was at the end of the old stack training.
        assert results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= min_return_old_stack
        if results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= min_return_new_stack:
            print(
                f"Reached episode return of {min_return_new_stack} -> stopping "
                "new API stack training."
            )
            passed = True
            break

    if not passed:
        raise ValueError(
            "Continuing training on the new stack did not succeed! Last return: "
            f"{results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]}"
        )
