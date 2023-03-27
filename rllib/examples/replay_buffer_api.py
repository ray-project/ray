# __sphinx_doc_replay_buffer_api_example_script_begin__
"""Simple example of how to modify replay buffer behaviour.

We modify R2D2 to utilize prioritized replay but supplying it with the
PrioritizedMultiAgentReplayBuffer instead of the standard MultiAgentReplayBuffer.
This is possible because R2D2 uses the DQN training iteration function,
which includes and a priority update, given that a fitting buffer is provided.
"""

import argparse

import ray
from ray import air, tune
from ray.rllib.algorithms.r2d2 import R2D2Config
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.replay_buffers.replay_buffer import StorageUnit

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()

parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--stop-iters", type=int, default=50, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    # This is where we add prioritized experiences replay
    # The training iteration function that is shared by DQN and R2D2 already
    # includes a priority update step.
    replay_buffer_config = {
        "type": "MultiAgentPrioritizedReplayBuffer",
        # Although not necessary, we can modify the default constructor args of
        # the replay buffer here
        "prioritized_replay_alpha": 0.5,
        "storage_unit": StorageUnit.SEQUENCES,
        "replay_burn_in": 20,
        "zero_init_states": True,
    }

    config = (
        R2D2Config()
        .environment("CartPole-v1")
        .framework(framework=args.framework)
        .rollouts(num_rollout_workers=4)
        .training(
            model=dict(use_lstm=True, lstm_cell_size=64, max_seq_len=20),
            replay_buffer_config=replay_buffer_config,
        )
    )

    stop_config = {
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    results = tune.Tuner(
        "R2D2", param_space=config.to_dict(), run_config=air.RunConfig(stop=stop_config)
    ).fit()

    ray.shutdown()

# __sphinx_doc_replay_buffer_api_example_script_end__
