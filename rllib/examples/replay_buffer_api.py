# @OldAPIStack

# __sphinx_doc_replay_buffer_api_example_script_begin__
"""Simple example of how to modify replay buffer behaviour.

We modify DQN to utilize prioritized replay but supplying it with the
PrioritizedMultiAgentReplayBuffer instead of the standard MultiAgentReplayBuffer.
This is possible because DQN uses the DQN training iteration function,
which includes and a priority update, given that a fitting buffer is provided.
"""

import argparse

import ray
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
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
    # The training iteration function that is used by DQN already includes a priority
    # update step.
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
        DQNConfig()
        .environment("CartPole-v1")
        .framework(framework=args.framework)
        .env_runners(num_env_runners=4)
        .training(
            model=dict(use_lstm=True, lstm_cell_size=64, max_seq_len=20),
            replay_buffer_config=replay_buffer_config,
        )
    )

    stop_config = {
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        TRAINING_ITERATION: args.stop_iters,
    }

    results = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop_config),
    ).fit()

    ray.shutdown()

# __sphinx_doc_replay_buffer_api_example_script_end__
