# __sphinx_doc_replay_buffer_api_example_script_begin__
"""Simple example of how to modify replay buffer behaviour.

We modify R2D2 to utilize prioritized replay but supplying it with the
PrioritizedMultiAgentReplayBuffer instead of the standard MultiAgentReplayBuffer.
This is possible because R2D2 uses the DQN training iteration function,
which includes and a priority update, given that a fitting buffer is provided.
"""

import argparse
import os

import ray
from ray import tune
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()

parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    config = {
        "env": "CartPole-v0",
        "model": {
            "use_lstm": True,
            "lstm_cell_size": 64,
            "max_seq_len": 20,
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
    }

    stop_config = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    # This is where we add prioritized experiences replay
    # The training iteration function that is shared by DQN and R2D2 already includes
    # a priority update step.
    config["replay_buffer_config"] = {
        "type": "MultiAgentPrioritizedReplayBuffer",
        # Although not necessary, we can modify the default constructor args of
        # the replay buffer here
        "prioritized_replay_alpha": 0.5,
        "replay_burn_in": 20,
    }

    results = tune.run("R2D2", config=config, stop=stop_config)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()

# __sphinx_doc_replay_buffer_api_example_script_end__
