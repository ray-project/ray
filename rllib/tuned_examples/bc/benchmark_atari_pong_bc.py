"""
schema={
      a_t: int64,
      r_t: float,
      episode_return: float,
      o_tp1: list<item: binary>,
      episode_id: int64,
      a_tp1: int64,
      o_t: list<item: binary>,
      d_t: float
   }
"""

# from google.cloud import storage
import gymnasium as gym
import io
import numpy as np

# from pathlib import Path
from PIL import Image
import tree
from typing import Optional

import pyarrow.fs

from ray.data import TFXReadOptions
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.annotations import override
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
)
from ray import tune


class DecodeObservations(ConnectorV2):
    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        multi_agent: bool = False,
        as_learner_connector: bool = True,
        **kwargs,
    ):
        """Decodes observation from PNG to numpy array.

        Note, `rl_unplugged`'s stored observations are framestacked with
        four frames per observation. This connector returns therefore
        decoded observations of shape `(84, 84, 4)`.

        Args:
            multi_agent: Whether this is a connector operating on a multi-agent
                observation space mapping AgentIDs to individual agents' observations.
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to an env-to-module pipeline.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self._multi_agent = multi_agent
        self._as_learner_connector = as_learner_connector

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module,
        data,
        episodes,
        explore=None,
        shared_data=None,
        **kwargs,
    ):

        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=False
        ):
            # Map encoded PNGs into arrays of shape (84, 84, 4).
            def _map_fn(s):
                return np.concatenate(
                    [
                        np.array(Image.open(io.BytesIO(s[i]))).reshape(84, 84, 1)
                        for i in range(4)
                    ],
                    axis=2,
                )

            # Add the observations for t.
            self.add_n_batch_items(
                batch=data,
                column=Columns.OBS,
                items_to_add=tree.map_structure(
                    _map_fn,
                    sa_episode.get_observations(slice(0, len(sa_episode))),
                ),
                num_items=len(sa_episode),
                single_agent_episode=sa_episode,
            )
            # Add the observations for t+1.
            self.add_n_batch_items(
                batch=data,
                column=Columns.NEXT_OBS,
                items_to_add=tree.map_structure(
                    _map_fn,
                    sa_episode.get_observations(slice(1, len(sa_episode) + 1)),
                ),
                num_items=len(sa_episode),
                single_agent_episode=sa_episode,
            )

        return data


# Make the learner connector.
def _make_learner_connector(observation_space, action_space):
    return DecodeObservations()


# Wrap the environment used in evalaution into `RLlib`'s Atari Wrapper
# that automatically stacks frames and converts to the dimension used
# in the collection of the `rl_unplugged` data.
def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make("ALE/Pong-v5", **cfg),
        # Perform frame-stacking through ConnectorV2 API.
        framestack=4,
        dim=84,
    )


# Register the wrapped environment to `tune`.
tune.register_env("WrappedALE/Pong-v5", _env_creator)

parser = add_rllib_example_script_args()
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

# RLUnplugged GCS bucket. This bucket contains for each set of environments
# (e.g. Atari) a directory and for each environment within. For each
# environment multiple runs were collected.
rlunplugged_base_path = "rl_unplugged/atari"

# We only use the Atari game `Pong` here. Users can choose other Atari
# games and set here the name. This will download `TfRecords` dataset from GCS.
game = "Pong"

# Path to the directory with all runs from Atari Pong.
rlunplugged_path = rlunplugged_base_path + f"/{game}"

# Set up the GCS file system.
filesystem = pyarrow.fs.GcsFileSystem(anonymous=True)
# There are many run numbers, we choose the first one for demonstration. This
# can be chosen by users. To use all data use a list of file paths (see
# `num_shards`) and its usage further below.
# run_number = 1

# Get all file infos to calculate an optimal number of blocks for reading.
all_file_infos = filesystem.get_file_info(pyarrow.fs.FileSelector(rlunplugged_path))
# Get the total file size
total_file_size_mb = sum(fi.size for fi in all_file_infos)
# A block is defined to be 128MB.
num_blocks = int(total_file_size_mb / 128) - 1

# # num_shards = 1


# # Make the temporary directory for the downloaded data.
# tmp_path = "/tmp/atari"
# Path(tmp_path).joinpath(game).mkdir(exist_ok=True, parents=True)
# destination_file_name = f"{tmp_path}/{game}/run_{run_number}-00000-of-00001"

# # If the file is not downloaded, yet, download it here.
# if not Path(destination_file_name).exists():
#     # Define the bucket and source file.
#     bucket_name = "rl_unplugged"
#     source_blob_name = f"atari/{game}/run_{run_number}-00000-of-00100"

#     # Download the data from the bucket.
#     storage_client = storage.Client.create_anonymous_client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#     blob.download_to_filename(destination_file_name)

# Define the config for Behavior Cloning.
config = (
    BCConfig()
    .environment(
        env="WrappedALE/Pong-v5",
        # TODO (sven): Does this have any influence in connectors?
        clip_rewards=True,
    )
    # Use the new API stack that makes directly use of `ray.data`.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    # Evaluate in the actual environment online.
    .evaluation(
        evaluation_interval=3,
        evaluation_num_env_runners=1,
        evaluation_duration=5,
        evaluation_parallel_to_training=True,
    )
    .learners(
        num_learners=args.num_gpus if args.num_gpus > 1 else 0,
        num_gpus_per_learner=0,
    )
    # Note, the `input_` argument is the major argument for the
    # new offline API. Via the `input_read_method_kwargs` the
    # arguments for the `ray.data.Dataset` read method can be
    # configured. The read method needs at least as many blocks
    # as remote learners.
    .offline_data(
        input_=["rl_unplugged/atari/Pong"],  # destination_file_name,
        input_read_method="read_tfrecords",
        input_read_method_kwargs={
            # Note, `TFRecords` datasets in `rl_unplugged` are GZIP
            # compressed and Arrow needs to decompress them.
            "arrow_open_stream_args": {"compression": "gzip"},
            # Use enough reading blocks to scale well.
            "override_num_blocks": num_blocks,
            # Read in parallel with these many actors.
            "concurrency": 80,
            # TFX improves performance extensively. `tfx-bsl` needs to be
            # installed for this.
            "tfx_read_options": TFXReadOptions(
                # batch_size=2000,
            ),
        },
        input_filesystem=filesystem,
        # `rl_unplugged`'s data schema is different from the one used
        # internally in `RLlib`. Define the schema here so it can be used
        # when transforming column data to episodes.
        input_read_schema={
            Columns.EPS_ID: "episode_id",
            Columns.OBS: "o_t",
            Columns.ACTIONS: "a_t",
            Columns.REWARDS: "r_t",
            Columns.NEXT_OBS: "o_tp1",
            Columns.TERMINATEDS: "d_t",
        },
        materialize_data=False,
        materialize_mapped_data=False,
        # Increase the parallelism in transforming batches, such that while
        # training, new batches are transformed while others are used in updating.
        map_batches_kwargs={
            "concurrency": 40,
            "num_cpus": 40,
        },  # max(args.num_gpus * 20, 20)},
        # When iterating over batches in the dataset, prefetch at least 20
        # batches per learner. Increase this for scaling out more.
        iter_batches_kwargs={
            "prefetch_batches": 4,  # max(args.num_gpus * 20, 20),
            "local_shuffle_buffer_size": None,
        },
        dataset_num_iters_per_learner=1,
    )
    .training(
        # To increase learning speed with multiple learners,
        # increase the learning rate correspondingly.
        lr=0.0008 * max(1, args.num_gpus**0.5),
        train_batch_size_per_learner=1024,
        # Use the defined learner connector above, to decode observations.
        learner_connector=_make_learner_connector,
    )
    .rl_module(
        model_config_dict={
            "vf_share_layers": True,
            "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            "conv_activation": "relu",
            "post_fcnet_hiddens": [256],
            "uses_new_env_runners": True,
        }
    )
)

# TODO (simon): Change to use the `run_rllib_example` function as soon as tuned.
algo = config.build()

for i in range(100):
    print(f"Iteration: {i + 1}")
    results = algo.train()
    print(results)
