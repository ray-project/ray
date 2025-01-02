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
import gymnasium as gym
import io
import numpy as np
import os

from PIL import Image
from typing import Optional
from ray import tune

from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.annotations import override
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
)


# Define a `ConnectorV2` to decode stacked encoded Atari frames.
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
                construct = [
                    np.array(Image.open(io.BytesIO(s[i]))).reshape(84, 84, 1)
                    for i in range(4)
                ]
                result = np.concatenate(construct, axis=2)
                return result

            # Add the observations for t.
            self.add_n_batch_items(
                batch=data,
                column=Columns.OBS,
                # Ensure, we pass in a list, otherwise it is considered
                # an already batched array.
                items_to_add=[
                    _map_fn(
                        sa_episode.get_observations(slice(0, len(sa_episode)))[0],
                    ).astype(np.float32)
                ],
                num_items=len(sa_episode),
                single_agent_episode=sa_episode,
            )
            # Add the observations for t+1.
            self.add_n_batch_items(
                batch=data,
                column=Columns.NEXT_OBS,
                items_to_add=[
                    _map_fn(
                        sa_episode.get_observations(slice(1, len(sa_episode) + 1))[0],
                    ).astype(np.float32)
                ],
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
        gym.make("ale_py:ALE/Pong-v5", **cfg),
        # Perform frame-stacking through ConnectorV2 API.
        framestack=4,
        dim=84,
    )


# Register the wrapped environment to `tune`.
tune.register_env("WrappedALE/Pong-v5", _env_creator)

# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
parser = add_rllib_example_script_args(
    default_reward=float("inf"),
    default_timesteps=3000000,
    default_iters=100000000000,
)
args = parser.parse_args()

# If multiple learners are requested define a scheduling
# strategy with best data locality.
if args.num_learners and args.num_learners > 1:
    import ray

    ray.init()
    # Check, if we have a multi-node cluster.
    nodes = ray.nodes()
    ray.shutdown()
    print(f"Number of nodes in cluster: {len(nodes)}")
    # If we have a multi-node cluster spread learners.
    if len(nodes) > 1:
        os.environ["TRAIN_ENABLE_WORKER_SPREAD_ENV"] = "1"
        print(
            "Multi-node cluster and multi-learner setup. "
            "Using a 'SPREAD' scheduling strategy for learners"
            "to support data locality."
        )
    # Otherwise pack the learners on the single node.
    else:
        print(
            "Single-node cluster and multi-learner setup. "
            "Using a 'PACK' scheduling strategy for learners"
            "to support data locality."
        )
# Anyscale RLUnplugged storage bucket. The bucket contains from the
# original `RLUnplugged` bucket only the first `atari/Pong` run.
anyscale_storage_bucket = os.environ["ANYSCALE_ARTIFACT_STORAGE"]
anyscale_rlunplugged_atari_path = anyscale_storage_bucket + "/rllib/rl_unplugged/atari"

# We only use the Atari game `Pong` here. Users can choose other Atari
# games and set here the name. This will download `TfRecords` dataset from GCS.
game = "Pong"

# Path to the directory with all runs from Atari Pong.
anyscale_rlunplugged_atari_pong_path = anyscale_rlunplugged_atari_path + f"/{game}"
print(
    "Streaming RLUnplugged Atari Pong data from path: "
    f"{anyscale_rlunplugged_atari_pong_path}"
)

# Define the config for Behavior Cloning.
config = (
    BCConfig()
    .environment(
        env="WrappedALE/Pong-v5",
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
        num_learners=args.num_learners
        if args.num_learners and args.num_learners > 1
        else 0,
        num_gpus_per_learner=args.num_gpus_per_learner,
    )
    # Note, the `input_` argument is the major argument for the
    # new offline API. Via the `input_read_method_kwargs` the
    # arguments for the `ray.data.Dataset` read method can be
    # configured. The read method needs at least as many blocks
    # as remote learners.
    .offline_data(
        input_=[anyscale_rlunplugged_atari_pong_path],
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
        # Do not materialize data, instead stream the data from Anyscale's
        # S3 bucket (note, streaming data is an Anyscale-platform-only feature).
        materialize_data=False,
        materialize_mapped_data=False,
        # Increase the parallelism in transforming batches, such that while
        # training, new batches are transformed while others are used in updating.
        map_batches_kwargs={
            "concurrency": 12,
        },
        # When iterating over batches in the dataset, prefetch at least 4
        # batches per learner.
        iter_batches_kwargs={
            "prefetch_batches": 4,
        },
        # Iterate over 10 batches per RLlib iteration if multiple learners
        # are used.
        dataset_num_iters_per_learner=100
        if args.num_learners and args.num_learners > 1
        else 1,
    )
    .training(
        # To increase learning speed with multiple learners,
        # increase the learning rate correspondingly.
        lr=0.0008
        * max(
            1,
            (args.num_learners if args.num_learners and args.num_learners > 1 else 1)
            ** 0.5,
        ),
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

# TODO (simon): Change to use the `run_rllib_example` function as soon as this works
# with Ray Tune.
algo = config.build()

for i in range(100):
    print(f"Iteration: {i + 1}")
    results = algo.train()
    print(results)

for i in range(4):
    print("FINISHED")
