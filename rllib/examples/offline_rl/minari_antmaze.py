import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.examples.offline_rl.classes.minari_offline_data import MinariOfflineData
from ray.rllib.examples.offline_rl.classes.minari_offline_prelearner import (
    MinariOfflinePreLearner,
)
from ray.rllib.examples.offline_rl.classes.flatten_observations_learner_connector import (
    FlattenObservations,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_timesteps=200000,
    default_reward=0.0,
)
parser.set_defaults(
    checkpoint_at_end=True,
    max_concurrent_trials=1,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


def _make_learner_connector(input_observation_space, input_action_space):
    # Create the learner connector.
    return FlattenObservations(input_observation_space, input_action_space)


config = (
    BCConfig()
    .environment(
        # Note, Ant Maze has a dict observation space that contains, next to
        # observations also the desired and achieved goals. This needs the
        # FlattenObservations connector to flatten these.
        # See https://robotics.farama.org/envs/maze/ant_maze/
        observation_space=gym.spaces.Dict(
            {
                "achieved_goal": gym.spaces.Box(-np.inf, np.inf, (2,), np.float64),
                "desired_goal": gym.spaces.Box(-np.inf, np.inf, (2,), np.float64),
                "observation": gym.spaces.Box(-np.inf, np.inf, (27,), np.float64),
            }
        ),
        action_space=gym.spaces.Box(-1.0, 1.0, (8,), np.float32),
    )
    .training(
        # Register the custom learner connector.
        learner_connector=_make_learner_connector,
    )
    .offline_data(
        # Note, the `MinariOfflineData` can directly download via `minari`
        # the dataset.
        input_="D4RL/antmaze/large-diverse-v1",
        # Use the custom `MinariOfflineData` class to pull directly from `minari`.
        offline_data_class=MinariOfflineData,
        materialize_data=args.in_memory,
        materialize_mapped_data=args.in_memory,
        # Do not flatten (read single transitions), but instead read the full
        # episodes from `minari`. Download the dataset, if not available, yet.
        input_read_method_kwargs={"flatten": False, "download": True},
        # Because we read full episodes, we can read a single one and buffer it.
        input_read_batch_size=1,
        # Run a single iteration per RLlib iteration to update the `RLModule`.
        dataset_num_iters_per_learner=1,
        # Read in episodes (not transitions). Note, in this case, we use a
        # replay buffer, to buffer episodes and sample from this buffer the batch.
        input_read_episodes=True,
        # Use the custom `MinariOfflinePreLearner` that can preprocess data from
        # the custom `MinariDatasource`.
        prelearner_class=MinariOfflinePreLearner,
        # Use 10 PreLearners, 1 CPU each.
        map_batches_kwargs={"concurrency": 10, "num_cpus": 1},
    )
)

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    results = run_rllib_example_script_experiment(config, args)
