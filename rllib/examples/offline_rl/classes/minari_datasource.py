import minari
import msgpack
import msgpack_numpy as mnp
import numpy as np
import pyarrow as pa

from gymnasium.core import ObsType
from typing import Any, Dict, List, Union

from ray.data.datasource import Datasource, ReadTask
from ray.rllib.core.columns import Columns


def _to_large_binary(obj) -> bytes:
    """Serialize arbitrary obj to bytes.

    Args:
        obj: An Python object that should be converted to bytes.

        Returns:
            A byte stream for the object `obj` converted with `msgpack`
            using `msgpack_numpy` for encoding.

    """
    return msgpack.packb(obj, default=mnp.encode, use_bin_type=True)


def _preprocess_observations(
    observations: Union[ObsType, Dict[str, ObsType]]
) -> List[np.array]:
    """Preprocess observations into a consistent list format.

    This utility handles two cases:
      1. If `observations` is a dictionary of sequences (e.g., arrays keyed by
         observation name), it converts them into a list of dictionaries,
         where each dictionary corresponds to one timestep/transition.
         Example:
             {"obs1": [a, b], "obs2": [x, y]}
             -> [{"obs1": a, "obs2": x}, {"obs1": b, "obs2": y}]
      2. If `observations` is already a sequence/array, it is returned as-is.

    Args:
        observations: Either a single sequence/array of observations, or a dictionary
            mapping keys to sequences of observations.

    Returns:
        A list of observations. Each element is either an array
        (if input was a sequence) or a dictionary of arrays (if input
        was a dict of sequences).
    """
    # Observations may be dict of arrays.
    if isinstance(observations, dict):
        # Convert dict-of-seqs to list-of-dicts (for each transition).
        return [
            dict(zip(observations.keys(), values))
            for values in zip(*observations.values())
        ]
    # Otherwise, use observations as they are.
    else:
        return observations


def _preprocess_infos(infos: Union[List, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Preprocess info data into a consistent list-of-dicts format.

    This utility handles two cases:
      1. If `infos` is a dictionary of sequences (e.g., lists/arrays keyed by
         info field), it converts them into a list of dictionaries, where each
         dictionary corresponds to one timestep/transition.
         Example:
             {"reward": [1, 2], "done": [False, True]}
             -> [{"reward": 1, "done": False}, {"reward": 2, "done": True}]
      2. If `infos` is already a list of dictionaries, it is returned unchanged.

    Args:
        infos: Either a list of dictionaries (already preprocessed), or a
            dictionary mapping keys to sequences of values.

    Returns:
        A list of dictionaries, where each dictionary contains info for a single
        timestep/transition.
    """
    # Infos is typically a dict of lists.
    if isinstance(infos, dict):
        # Convert the dict-of-seqs into a list-of-dicts (for each transition).
        return [dict(zip(infos.keys(), values)) for values in zip(*infos.values())]
    # Otherwise, use infos as they are.
    else:
        return infos


class MinariDatasource(Datasource):
    def __init__(self, dataset_id: str, flatten: bool = True, download: bool = False):
        """Ray Data DataSource for Minari datasets.

        Args:
            dataset_id: The HF Minari dataset id, e.g. "mujoco/ant/expert-v0".
            flatten: If True, yield transitions; if False, yield whole episodes.
            download: If True, download the data to disk. Note, each dataset needs to
                be downloaded first before it can be used.
        """
        self.dataset_id = dataset_id
        self.flatten = flatten
        self.download = download

    def get_name(self):
        return f"MinariDatasource({self.dataset_id})"

    def prepare_read(self, parallelism: int, **kwargs) -> List["ReadTask"]:
        # Load dataset once (on driver).
        minari_ds = minari.load_dataset(self.dataset_id, download=self.download)
        # Get a list of Minari episodes.
        episodes = list(minari_ds.iterate_episodes())

        # Split into shards
        n = len(episodes)
        shard_size = (n + parallelism - 1) // parallelism
        shards = [episodes[i : i + shard_size] for i in range(0, n, shard_size)]

        def make_read_task(shard):
            def read_fn():
                # If episodes should be "flattened" into single transitions.
                if self.flatten:
                    # Build struct column "transition".
                    (
                        eid_arr,
                        obs_bin,
                        act_bin,
                        rew_arr,
                        infos_bin,
                        next_obs_bin,
                        term_arr,
                        trunc_arr,
                    ) = ([], [], [], [], [], [], [], [])

                    # Loop over all episodes in the actual shard.
                    for ep in shard:
                        # Preprocess observations and infos b/c they are not sorted per transition.
                        observations = _preprocess_observations(ep.observations)
                        infos = _preprocess_infos(ep.infos)

                        # Iterate per-transition.
                        for obs, act, rew, info, next_obs, term, trunc in zip(
                            observations[:-1],
                            ep.actions,
                            ep.rewards,
                            observations[1:],
                            infos[:-1],
                            ep.terminations,
                            ep.truncations,
                        ):
                            obs_bin.append(_to_large_binary(obs))
                            act_bin.append(_to_large_binary(act))
                            rew_arr.append(float(rew))
                            infos_bin.append(_to_large_binary(info))
                            next_obs_bin.append(_to_large_binary(next_obs))
                            term_arr.append(bool(term))
                            trunc_arr.append(bool(trunc))
                            eid_arr.append(str(ep.id))

                    # If we have no observations, return an empty struct.
                    # TODO (simon): Check, if this is needed.
                    if not obs_bin:
                        # Return empty table with the expected schema
                        struct_type = pa.struct(
                            [
                                (Columns.OBS, pa.large_binary()),
                                (Columns.ACTIONS, pa.large_binary()),
                                (Columns.REWARDS, pa.float32()),
                                (Columns.INFOS, pa.large_binary()),
                                (Columns.NEXT_OBS, pa.large_binary()),
                                (Columns.TERMINATEDS, pa.bool_()),
                                (Columns.TRUNCATEDS, pa.bool_()),
                                (Columns.EPS_ID, pa.string()),
                            ]
                        )
                        empty_struct = pa.StructArray.from_arrays(
                            [pa.array([], type=t) for _, t in struct_type],
                            fields=list(struct_type),
                        )
                        table = pa.Table.from_arrays(
                            [empty_struct], names=["transition"]
                        )
                        return [table]

                    # We need to return an Arrow table and need a PyArrow StructArray for this.
                    transition_struct = pa.StructArray.from_arrays(
                        [
                            pa.array(obs_bin, type=pa.large_binary()),
                            pa.array(act_bin, type=pa.large_binary()),
                            pa.array(rew_arr, type=pa.float32()),
                            pa.array(infos_bin, type=pa.large_binary()),
                            pa.array(next_obs_bin, type=pa.large_binary()),
                            pa.array(term_arr, type=pa.bool_()),
                            pa.array(trunc_arr, type=pa.bool_()),
                            pa.array(eid_arr, type=pa.string()),
                        ],
                        fields=[
                            pa.field(Columns.OBS, pa.large_binary()),
                            pa.field(Columns.ACTIONS, pa.large_binary()),
                            pa.field(Columns.REWARDS, pa.float32()),
                            pa.field(Columns.INFOS, pa.large_binary()),
                            pa.field(Columns.NEXT_OBS, pa.large_binary()),
                            pa.field(Columns.TERMINATEDS, pa.bool_()),
                            pa.field(Columns.TRUNCATEDS, pa.bool_()),
                            pa.field(Columns.EPS_ID, pa.string()),
                        ],
                    )

                    table = pa.Table.from_arrays(
                        [transition_struct], names=["transition"]
                    )
                    return [table]
                # Otherwise, we return complete episodes as we receive them from Minari.
                else:
                    # Build struct column "episodes". Normalize episode fields into bytes/bools.
                    (
                        ep_id,
                        obs_bin,
                        infos_bin,
                        acts_bin,
                        rews_bin,
                        term_arr,
                        trunc_arr,
                    ) = (
                        [],
                        [],
                        [],
                        [],
                        [],
                        [],
                        [],
                    )

                    # Loop over Minari episodes, but this time pack them as complete struct into
                    # the Arrow table.
                    for ep in shard:
                        # Preprocess observations and infos b/c they are not sorted per transition.
                        observations = _preprocess_observations(ep.observations)
                        infos = _preprocess_infos(ep.infos)

                        # actions/rewards likely ndarray/list
                        actions = ep.actions
                        rewards = ep.rewards

                        # Build the episode struct.
                        terminated = bool(ep.terminations[-1])
                        truncated = bool(ep.truncations[-1])

                        ep_id.append(str(ep.id))
                        obs_bin.append(_to_large_binary(observations))
                        infos_bin.append(_to_large_binary(infos))
                        acts_bin.append(_to_large_binary(actions))
                        rews_bin.append(_to_large_binary(rewards))
                        term_arr.append(terminated)
                        trunc_arr.append(truncated)

                    # If no episode ID exists, return an empty struct.
                    # TODO (simon): Check, if this is indeed needed.
                    if not ep_id:
                        struct_type = pa.struct(
                            [
                                (Columns.EPS_ID, pa.string()),
                                (Columns.OBS, pa.large_binary()),
                                (Columns.ACTIONS, pa.large_binary()),
                                (Columns.REWARDS, pa.large_binary()),
                                (Columns.INFOS, pa.large_binary()),
                                (Columns.TERMINATEDS, pa.bool_()),
                                (Columns.TRUNCATEDS, pa.bool_()),
                            ]
                        )
                        empty_struct = pa.StructArray.from_arrays(
                            [pa.array([], type=t) for _, t in struct_type],
                            fields=list(struct_type),
                        )
                        table = pa.Table.from_arrays([empty_struct], names=["episodes"])
                        return [table]

                    # Create the PyArrow structured array to return in the PyArrow table.
                    episode_struct = pa.StructArray.from_arrays(
                        [
                            pa.array(ep_id, type=pa.string()),
                            pa.array(obs_bin, type=pa.large_binary()),
                            pa.array(acts_bin, type=pa.large_binary()),
                            pa.array(rews_bin, type=pa.large_binary()),
                            pa.array(infos_bin, type=pa.large_binary()),
                            pa.array(term_arr, type=pa.bool_()),
                            pa.array(trunc_arr, type=pa.bool_()),
                        ],
                        fields=[
                            pa.field(Columns.EPS_ID, pa.string()),
                            pa.field(Columns.OBS, pa.large_binary()),
                            pa.field(Columns.ACTIONS, pa.large_binary()),
                            pa.field(Columns.REWARDS, pa.large_binary()),
                            pa.field(Columns.INFOS, pa.large_binary()),
                            pa.field(Columns.TERMINATEDS, pa.bool_()),
                            pa.field(Columns.TRUNCATEDS, pa.bool_()),
                        ],
                    )

                    # We need to return a PyArrow table for each shard.
                    table = pa.Table.from_arrays([episode_struct], names=["episodes"])
                    return [table]

            # prepare_read needs to return a `ReadTask`.
            return ReadTask(read_fn, metadata=None)

        return [make_read_task(shard) for shard in shards]
