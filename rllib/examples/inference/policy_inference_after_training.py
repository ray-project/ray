"""Example on how to compute actions in production on an already trained policy.

This example uses the simplest setup possible: An RLModule (policy net) recovered
from a checkpoint and a manual env-loop (CartPole-v1). No ConnectorV2s or EnvRunners are
used in this example.

This example:
    - shows how to use an already existing checkpoint to extract a single-agent RLModule
    from (our policy network).
    - shows how to setup this recovered policy net for action computations (with or
    without using exploration).
    - shows have the policy run through a very simple gymnasium based env-loop, w/o
    using RLlib's ConnectorV2s or EnvRunners.


How to run this script
----------------------
`python [script file name].py --stop-reward=200.0`

Use the `--use-onnx-for-inference` option to perform action computations after training
through an ONNX runtime session.
Use the `--explore-during-inference` option to switch on exploratory behavior
during inference. Normally, you should not explore during inference, though,
unless your environment has a stochastic optimal solution. Note also that this option
doesn't work in combination with the `--use-onnx-for-inference` option.
Use the `--num-episodes-during-inference=[int]` option to set the number of
episodes to run through during the inference phase using the restored RLModule.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

Note that the shown GPU settings in this script also work in case you are not
running via tune, but instead are using the `--no-tune` command line option.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.


Results to expect
-----------------

For the training step - depending on your `--stop-reward` setting, you should see
something similar to this:

Number of trials: 1/1 (1 TERMINATED)
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_6660c_00000 | TERMINATED | 127.0.0.1:43566 |      8 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |   num_env_steps_sample |   num_env_steps_traine |
|                  |             d_lifetime |             d_lifetime |
+------------------+------------------------+------------------------+
|          21.0283 |                  32000 |                  32000 |
+------------------+------------------------+------------------------+

Then, after restoring the RLModule for the inference phase, your output should
look similar to:

Training completed. Restoring new RLModule for action inference.
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Episode done: Total reward = 500.0
Done performing action inference through 10 Episodes
"""
import os

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy, softmax
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

torch, _ = try_import_torch()

parser = add_rllib_example_script_args(default_reward=200.0)
parser.add_argument(
    "--use-onnx-for-inference",
    action="store_true",
    help="Whether to convert the loaded module to ONNX format and then perform "
    "inference through this ONNX model.",
)
parser.add_argument(
    "--explore-during-inference",
    action="store_true",
    help="Whether the trained policy should use exploration during action "
    "inference.",
)
parser.add_argument(
    "--num-episodes-during-inference",
    type=int,
    default=10,
    help="Number of episodes to do inference over (after restoring from a checkpoint).",
)
parser.set_defaults(
    # Make sure that - by default - we produce checkpoints during training.
    checkpoint_freq=1,
    checkpoint_at_end=True,
    # Use CartPole-v1 by default.
    env="CartPole-v1",
)


if __name__ == "__main__":
    args = parser.parse_args()

    if args.use_onnx_for_inference:
        if args.explore_during_inference:
            raise ValueError(
                "Can't set `--explore-during-inference` and `--use-onnx-for-inference` "
                "together! ONNX models use the original RLModule's `forward_inference` "
                "only."
            )
        import onnxruntime

    base_config = get_trainable_cls(args.algo).get_default_config()

    print("Training policy until desired reward/timesteps/iterations. ...")
    results = run_rllib_example_script_experiment(base_config, args)

    print("Training completed. Restoring new RLModule for action inference.")
    # Get the last checkpoint from the above training run.
    best_result = results.get_best_result(
        metric=f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}", mode="max"
    )

    # Create new RLModule and restore its state from the last algo checkpoint.
    # Note that the checkpoint for the RLModule can be found deeper inside the algo
    # checkpoint's subdirectories ([algo dir] -> "learner/" -> "module_state/" ->
    # "[module ID]):
    print("Restore RLModule from checkpoint ...", end="")
    rl_module = RLModule.from_checkpoint(
        os.path.join(
            best_result.checkpoint.path,
            "learner_group",
            "learner",
            "rl_module",
            DEFAULT_MODULE_ID,
        )
    )
    ort_session = None
    print(" ok")

    # Create an env to do inference in.
    env = gym.make(args.env)
    obs, info = env.reset()

    num_episodes = 0
    episode_return = 0.0

    while num_episodes < args.num_episodes_during_inference:
        # Compute an action using a B=1 observation "batch".
        input_dict = {Columns.OBS: np.expand_dims(obs, 0)}
        if not args.use_onnx_for_inference:
            input_dict = {Columns.OBS: torch.from_numpy(obs).unsqueeze(0)}

        # If ONNX and module has not been exported yet, do this here using
        # the input_dict as example input.
        elif ort_session is None:
            tensor_input_dict = {Columns.OBS: torch.from_numpy(obs).unsqueeze(0)}
            torch.onnx.export(rl_module, {"batch": tensor_input_dict}, f="test.onnx")
            ort_session = onnxruntime.InferenceSession(
                "test.onnx", providers=["CPUExecutionProvider"]
            )

        # No exploration (using ONNX).
        if ort_session is not None:
            rl_module_out = ort_session.run(
                None,
                {
                    key.name: val
                    for key, val in dict(
                        zip(
                            tree.flatten(ort_session.get_inputs()),
                            tree.flatten(input_dict),
                        )
                    ).items()
                },
            )
            # [0]=encoder outs; [1]=action logits
            rl_module_out = {Columns.ACTION_DIST_INPUTS: rl_module_out[1]}
        # No exploration (using RLModule).
        elif not args.explore_during_inference:
            rl_module_out = rl_module.forward_inference(input_dict)
        # W/ exploration (using RLModule).
        else:
            rl_module_out = rl_module.forward_exploration(input_dict)

        # For discrete action spaces used here, normally, an RLModule "only"
        # produces action logits, from which we then have to sample.
        # However, you can also write custom RLModules that output actions
        # directly, performing the sampling step already inside their
        # `forward_...()` methods.
        logits = convert_to_numpy(rl_module_out[Columns.ACTION_DIST_INPUTS])
        # Perform the sampling step in numpy for simplicity.
        action = np.random.choice(env.action_space.n, p=softmax(logits[0]))
        # Send the computed action `a` to the env.
        obs, reward, terminated, truncated, _ = env.step(action)
        episode_return += reward
        # Is the episode `done`? -> Reset.
        if terminated or truncated:
            print(f"Episode done: Total reward = {episode_return}")
            obs, info = env.reset()
            num_episodes += 1
            episode_return = 0.0

    print(f"Done performing action inference through {num_episodes} Episodes")
