# @OldAPIStack
"""
Example showing how you can use your trained policy for inference
(computing actions) in an environment.

Includes options for LSTM-based models (--use-lstm), attention-net models
(--use-attention), and plain (non-recurrent) models.
"""
import argparse
import os

import gymnasium as gym
import numpy as np

import ray
from ray import tune
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune.registry import get_trainable_cls
from ray.tune.result import TRAINING_ITERATION

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--prev-n-actions",
    type=int,
    default=0,
    help="Feed n most recent actions to the attention net as part of its input.",
)
parser.add_argument(
    "--prev-n-rewards",
    type=int,
    default=0,
    help="Feed n most recent rewards to the attention net as part of its input.",
)
parser.add_argument(
    "--stop-iters",
    type=int,
    default=200,
    help="Number of iterations to train before we do inference.",
)
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=100000,
    help="Number of timesteps to train before we do inference.",
)
parser.add_argument(
    "--stop-reward",
    type=float,
    default=150.0,
    help="Reward at which we stop training before we do inference.",
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
    help="Number of episodes to do inference over after training.",
)
parser.add_argument(
    "--use-onnx-for-inference",
    action="store_true",
    help="Whether to convert the loaded module to ONNX format and then perform "
    "inference through this ONNX model.",
)

if __name__ == "__main__":
    args = parser.parse_args()

    if args.use_onnx_for_inference:
        if args.explore_during_inference:
            raise ValueError(
                "Can't set `--explore-during-inference` and `--use-onnx-for-inference` together!"
            )
        import onnxruntime

    ray.init(num_cpus=args.num_cpus or None)

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .api_stack(
            enable_env_runner_and_connector_v2=False,
            enable_rl_module_and_learner=False,
        )
        .environment("FrozenLake-v1")
        # Run with tracing enabled for tf2?
        .framework(args.framework)
        .training(
            model={
                "use_attention": True,
                "attention_num_transformer_units": 1,
                "attention_use_n_prev_actions": args.prev_n_actions,
                "attention_use_n_prev_rewards": args.prev_n_rewards,
                "attention_dim": 32,
                "attention_memory_inference": 10,
                "attention_memory_training": 10,
            },
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        TRAINING_ITERATION: args.stop_iters,
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    }

    print("Training policy until desired reward/timesteps/iterations. ...")
    tuner = tune.Tuner(
        args.run,
        param_space=config,
        run_config=tune.RunConfig(
            stop=stop,
            verbose=2,
            checkpoint_config=tune.CheckpointConfig(
                checkpoint_frequency=1,
                checkpoint_at_end=True,
            ),
        ),
    )
    results = tuner.fit()

    print("Training completed. Restoring new Algorithm for action inference.")
    # Get the last checkpoint from the above training run.
    checkpoint = results.get_best_result().checkpoint
    # Create new Algorithm and restore its state from the last checkpoint.
    algo = Algorithm.from_checkpoint(checkpoint)
    # Export ONNX model if relevant
    if args.use_onnx_for_inference:
        algo.get_policy().export_model(
            "frozenlake_attention_model_onnx",
            # ONNX opset version 12 required to support einsum operator.
            # Requires ONNX >= 1.7 and ONNX runtime >= 1.3
            onnx=12,
        )

    # Create the env to do inference in.
    env = gym.make("FrozenLake-v1")
    obs, info = env.reset()

    # In case the model needs previous-reward/action inputs, keep track of
    # these via these variables here (we'll have to pass them into the
    # compute_actions methods below).
    init_prev_a = prev_a = None
    init_prev_r = prev_r = None

    # Set attention net's initial internal state.
    num_transformers = config["model"]["attention_num_transformer_units"]
    memory_inference = config["model"]["attention_memory_inference"]
    attention_dim = config["model"]["attention_dim"]
    init_state = state = [
        np.zeros([memory_inference, attention_dim], np.float32)
        for _ in range(num_transformers)
    ]
    # Do we need prev-action/reward as part of the input?
    if args.prev_n_actions:
        init_prev_a = prev_a = np.array([0] * args.prev_n_actions)
    if args.prev_n_rewards:
        init_prev_r = prev_r = np.array([0.0] * args.prev_n_rewards)

    num_episodes = 0
    ort_session = None

    while num_episodes < args.num_episodes_during_inference:

        # Compute an action (`a`).
        if args.use_onnx_for_inference:
            # Prepare the ONNX runtime session.
            if ort_session is None:
                ort_session = onnxruntime.InferenceSession(
                    "frozenlake_attention_model_onnx/model.onnx"
                )
            # Prepare the inputs dict.
            seq_len = np.array([config["model"]["max_seq_len"]], dtype=np.int32)

            # pre-process observation: obs is an integer.
            # we need to convert it to a one-hot vector (FrozenLake-v1 space).
            n = env.observation_space.n
            obs_one_hot = np.zeros(n, dtype=np.float32)
            obs_one_hot[obs] = 1.0
            obs = obs_one_hot
            # Add batch dimension.
            obs = np.array(obs, dtype=np.float32)[np.newaxis, :]

            state_ins = np.array(state, dtype=np.float32)

            ort_inputs = {
                "obs": obs,
                "state_ins": state_ins,
                "seq_lens": seq_len,
            }

            if init_prev_a is not None:
                ort_inputs["prev_actions"] = prev_a.astype(np.int64)[np.newaxis, :]
            if init_prev_r is not None:
                ort_inputs["prev_rewards"] = prev_r.astype(np.float32)[np.newaxis, :]
            # Run the ONNX model.
            ort_outs = ort_session.run(
                output_names=["output", "state_outs"],
                input_feed=ort_inputs,
            )
            # Extract action and state-out from the ONNX model outputs.
            dist_inputs = ort_outs[0][0]
            # Exploration could be added here based on `dist_inputs`.
            # This would require using the configured exploration strategy.
            # Not implemented in this example.
            a = np.argmax(dist_inputs)

            state_out = [ort_outs[i + 1][0] for i in range(len(state))]
        else:
            a, state_out, _ = algo.compute_single_action(
                observation=obs,
                state=state,
                prev_action=prev_a,
                prev_reward=prev_r,
                explore=args.explore_during_inference,
                policy_id="default_policy",  # <- default value
            )
        # Send the computed action `a` to the env.
        obs, reward, done, truncated, _ = env.step(a)
        # Is the episode `done`? -> Reset.
        if done:
            obs, info = env.reset()
            num_episodes += 1
            state = init_state
            prev_a = init_prev_a
            prev_r = init_prev_r
        # Episode is still ongoing -> Continue.
        else:
            # Append the just received state-out (most recent timestep) to the
            # cascade (memory) of our state-ins and drop the oldest state-in.
            state = [
                np.concatenate([state[i], [state_out[i]]], axis=0)[1:]
                for i in range(num_transformers)
            ]
            if init_prev_a is not None:
                prev_a = np.concatenate([prev_a, [a]])[1:]
            if init_prev_r is not None:
                prev_r = np.concatenate([prev_r, [reward]])[1:]

    algo.stop()

    ray.shutdown()
