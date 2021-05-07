import collections
import numpy as np
import sys
import itertools
from typing import Dict, List, Optional, Set, Union

from ray.util import log_once
from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI
from ray.rllib.utils.compression import pack, unpack, is_compressed
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.memory import concat_aligned
from ray.rllib.utils.typing import PolicyID, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

# Default policy id for single agent environments
DEFAULT_POLICY_ID = "default_policy"


@PublicAPI
class SampleBatch(dict):
    """Wrapper around a dictionary with string keys and array-like values.

    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    # Outputs from interacting with the environment
    OBS = "obs"
    CUR_OBS = "obs"
    NEXT_OBS = "new_obs"
    ACTIONS = "actions"
    REWARDS = "rewards"
    PREV_ACTIONS = "prev_actions"
    PREV_REWARDS = "prev_rewards"
    DONES = "dones"
    INFOS = "infos"

    # Extra action fetches keys.
    ACTION_DIST_INPUTS = "action_dist_inputs"
    ACTION_PROB = "action_prob"
    ACTION_LOGP = "action_logp"

    # Uniquely identifies an episode.
    EPS_ID = "eps_id"

    # Uniquely identifies a sample batch. This is important to distinguish RNN
    # sequences from the same episode when multiple sample batches are
    # concatenated (fusing sequences across batches can be unsafe).
    UNROLL_ID = "unroll_id"

    # Uniquely identifies an agent within an episode.
    AGENT_INDEX = "agent_index"

    # Value function predictions emitted by the behaviour policy.
    VF_PREDS = "vf_preds"

    @PublicAPI
    def __init__(self, *args, **kwargs):
        """Constructs a sample batch (same params as dict constructor)."""

        # Possible seq_lens (TxB or BxT) setup.
        self.time_major = kwargs.pop("_time_major", None)

        self.max_seq_len = kwargs.pop("_max_seq_len", None)
        self.zero_padded = kwargs.pop("_zero_padded", False)
        self.is_training = kwargs.pop("_is_training", None)

        # Call super constructor. This will make the actual data accessible
        # by column name (str) via e.g. self["some-col"].
        dict.__init__(self, *args, **kwargs)

        self.accessed_keys = set()
        self.added_keys = set()
        self.deleted_keys = set()
        self.intercepted_values = {}
        self.get_interceptor = None

        # Clear out None seq-lens.
        if self.get("seq_lens") is None or self.get("seq_lens") == []:
            self.pop("seq_lens", None)
        # Numpyfy seq_lens if list.
        elif isinstance(self.get("seq_lens"), list):
            self["seq_lens"] = np.array(self["seq_lens"], dtype=np.int32)

        if self.max_seq_len is None and self.get("seq_lens") is not None and \
                not (tf and tf.is_tensor(self["seq_lens"])) and \
                len(self["seq_lens"]) > 0:
            self.max_seq_len = max(self["seq_lens"])

        if self.is_training is None:
            self.is_training = self.pop("is_training", False)

        lengths = []
        copy_ = {k: v for k, v in self.items() if k != "seq_lens"}
        for k, v in copy_.items():
            assert isinstance(k, str), self
            len_ = len(v) if isinstance(
                v,
                (list, np.ndarray)) or (torch and torch.is_tensor(v)) else None
            lengths.append(len_)
            if isinstance(v, list):
                self[k] = np.array(v)

        if self.get("seq_lens") is not None and \
                not (tf and tf.is_tensor(self["seq_lens"])) and \
                len(self["seq_lens"]) > 0:
            self.count = sum(self["seq_lens"])
        else:
            self.count = lengths[0] if lengths else 0

    @PublicAPI
    def __len__(self):
        """Returns the amount of samples in the sample batch."""
        return self.count

    @staticmethod
    @PublicAPI
    def concat_samples(samples: List["SampleBatch"]) -> \
            Union["SampleBatch", "MultiAgentBatch"]:
        """Concatenates n data dicts or MultiAgentBatches.

        Args:
            samples (List[Dict[TensorType]]]): List of dicts of data (numpy).

        Returns:
            Union[SampleBatch, MultiAgentBatch]: A new (compressed)
                SampleBatch or MultiAgentBatch.
        """
        if isinstance(samples[0], MultiAgentBatch):
            return MultiAgentBatch.concat_samples(samples)
        seq_lens = []
        concat_samples = []
        zero_padded = samples[0].zero_padded
        max_seq_len = samples[0].max_seq_len
        for s in samples:
            if s.count > 0:
                assert s.zero_padded == zero_padded
                if zero_padded:
                    assert s.max_seq_len == max_seq_len
                concat_samples.append(s)
                if s.get("seq_lens") is not None:
                    seq_lens.extend(s["seq_lens"])

        out = {}
        for k in concat_samples[0].keys():
            out[k] = concat_aligned(
                [s[k] for s in concat_samples],
                time_major=concat_samples[0].time_major)
        return SampleBatch(
            out,
            seq_lens=seq_lens,
            _time_major=concat_samples[0].time_major,
            _zero_padded=zero_padded,
            _max_seq_len=max_seq_len,
        )

    @PublicAPI
    def concat(self, other: "SampleBatch") -> "SampleBatch":
        """Returns a new SampleBatch with each data column concatenated.

        Args:
            other (SampleBatch): The other SampleBatch object to concat to this
                one.

        Returns:
            SampleBatch: The new SampleBatch, resulting from concating `other`
                to `self`.

        Examples:
            >>> b1 = SampleBatch({"a": [1, 2]})
            >>> b2 = SampleBatch({"a": [3, 4, 5]})
            >>> print(b1.concat(b2))
            {"a": [1, 2, 3, 4, 5]}
        """

        if self.keys() != other.keys():
            raise ValueError(
                "SampleBatches to concat must have same columns! {} vs {}".
                format(list(self.keys()), list(other.keys())))
        out = {}
        for k in self.keys():
            out[k] = concat_aligned([self[k], other[k]])
        return SampleBatch(out)

    @PublicAPI
    def copy(self, shallow: bool = False) -> "SampleBatch":
        """Creates a (deep) copy of this SampleBatch and returns it.

        Args:
            shallow (bool): Whether the copying should be done shallowly.

        Returns:
            SampleBatch: A (deep) copy of this SampleBatch object.
        """
        copy_ = SampleBatch(
            {
                k: np.array(v, copy=not shallow)
                if isinstance(v, np.ndarray) else v
                for (k, v) in self.items()
            },
            seq_lens=self.get("seq_lens"),
        )
        copy_.set_get_interceptor(self.get_interceptor)
        return copy_

    @PublicAPI
    def rows(self) -> Dict[str, TensorType]:
        """Returns an iterator over data rows, i.e. dicts with column values.

        Yields:
            Dict[str, TensorType]: The column values of the row in this
                iteration.

        Examples:
            >>> batch = SampleBatch({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> for row in batch.rows():
                   print(row)
            {"a": 1, "b": 4}
            {"a": 2, "b": 5}
            {"a": 3, "b": 6}
        """

        for i in range(self.count):
            row = {}
            for k in self.keys():
                row[k] = self[k][i]
            yield row

    @PublicAPI
    def columns(self, keys: List[str]) -> List[any]:
        """Returns a list of the batch-data in the specified columns.

        Args:
            keys (List[str]): List of column names fo which to return the data.

        Returns:
            List[any]: The list of data items ordered by the order of column
                names in `keys`.

        Examples:
            >>> batch = SampleBatch({"a": [1], "b": [2], "c": [3]})
            >>> print(batch.columns(["a", "b"]))
            [[1], [2]]
        """

        out = []
        for k in keys:
            out.append(self[k])
        return out

    @PublicAPI
    def shuffle(self) -> None:
        """Shuffles the rows of this batch in-place."""

        permutation = np.random.permutation(self.count)
        for key, val in self.items():
            self[key] = val[permutation]

    @PublicAPI
    def split_by_episode(self) -> List["SampleBatch"]:
        """Splits this batch's data by `eps_id`.

        Returns:
            List[SampleBatch]: List of batches, one per distinct episode.
        """

        # No eps_id in data -> Make sure there are no "dones" in the middle
        # and add eps_id automatically.
        if SampleBatch.EPS_ID not in self:
            if SampleBatch.DONES in self:
                assert not any(self[SampleBatch.DONES][:-1])
            self[SampleBatch.EPS_ID] = np.repeat(0, self.count)
            return [self]

        slices = []
        cur_eps_id = self[SampleBatch.EPS_ID][0]
        offset = 0
        for i in range(self.count):
            next_eps_id = self[SampleBatch.EPS_ID][i]
            if next_eps_id != cur_eps_id:
                slices.append(self.slice(offset, i))
                offset = i
                cur_eps_id = next_eps_id
        slices.append(self.slice(offset, self.count))
        for s in slices:
            slen = len(set(s[SampleBatch.EPS_ID]))
            assert slen == 1, (s, slen)
        assert sum(s.count for s in slices) == self.count, (slices, self.count)
        return slices

    @PublicAPI
    def slice(self, start: int, end: int, state_start=None,
              state_end=None) -> "SampleBatch":
        """Returns a slice of the row data of this batch (w/o copying).

        Args:
            start (int): Starting index. If < 0, will zero-pad.
            end (int): Ending index.

        Returns:
            SampleBatch: A new SampleBatch, which has a slice of this batch's
                data.
        """
        if self.get("seq_lens") is not None and len(self["seq_lens"]) > 0:
            if start < 0:
                data = {
                    k: np.concatenate([
                        np.zeros(
                            shape=(-start, ) + v.shape[1:], dtype=v.dtype),
                        v[0:end]
                    ])
                    for k, v in self.items() if k != "seq_lens"
                }
            else:
                data = {
                    k: v[start:end]
                    for k, v in self.items() if k != "seq_lens"
                }
            if state_start is not None:
                assert state_end is not None
                state_idx = 0
                state_key = "state_in_{}".format(state_idx)
                while state_key in self:
                    data[state_key] = self[state_key][state_start:state_end]
                    state_idx += 1
                    state_key = "state_in_{}".format(state_idx)
                seq_lens = list(self["seq_lens"][state_start:state_end])
            else:
                # Fix state_in_x data.
                count = 0
                state_start = None
                seq_lens = None
                for i, seq_len in enumerate(self["seq_lens"]):
                    count += seq_len
                    if count >= end:
                        state_idx = 0
                        state_key = "state_in_{}".format(state_idx)
                        if state_start is None:
                            state_start = i
                        while state_key in self:
                            data[state_key] = self[state_key][state_start:i +
                                                              1]
                            state_idx += 1
                            state_key = "state_in_{}".format(state_idx)
                        seq_lens = list(self["seq_lens"][state_start:i]) + [
                            seq_len - (count - end)
                        ]
                        if start < 0:
                            seq_lens[0] += -start
                        assert sum(seq_lens) == (end - start)
                        break
                    elif state_start is None and count > start:
                        state_start = i

            return SampleBatch(
                data,
                seq_lens=seq_lens,
                _time_major=self.time_major,
            )
        else:
            return SampleBatch(
                {k: v[start:end]
                 for k, v in self.items()},
                _is_training=self.is_training,
                _time_major=self.time_major)

    @PublicAPI
    def timeslices(self, size=None, num_slices=None,
                   k: Optional[int] = None) -> List["SampleBatch"]:
        """Returns SampleBatches, each one representing a k-slice of this one.

        Will start from timestep 0 and produce slices of size=k.

        Args:
            size (int): The size (in timesteps) of each returned SampleBatch.
            num_slices (int): The number of slices to produce.
            k (int): Obsoleted: Use size or num_slices instead!
                The size (in timesteps) of each returned SampleBatch.

        Returns:
            List[SampleBatch]: The list of (new) SampleBatches (each one of
                size k).
        """
        if size is None and num_slices is None:
            deprecation_warning("k", "size and num_slices")
            assert k is not None
            size = k

        slices, state_slices = self._get_slice_indices(size)
        if len(state_slices) == 0:
            timeslices = [self.slice(i, j) for i, j in slices]
        else:
            timeslices = [
                self.slice(i, j, si, sj) for (i, j), (si, sj) in slices
            ]
        return timeslices

    def zero_pad(self, max_seq_len: int, exclude_states: bool = True):
        """Left zero-pad the data in this SampleBatch in place.

        This will set the `self.zero_padded` flag to True and
        `self.max_seq_len` to the given `max_seq_len` value.

        Args:
            max_len (int): The max (total) length to zero pad to.
            exclude_states (bool): If False, also zero-pad all `state_in_x`
                data. If False, leave `state_in_x` keys as-is.
        """
        for col in self.keys():
            # Skip "state_in_..." columns and "seq_lens".
            if (exclude_states is True and col.startswith("state_in_")) or \
                    col == "seq_lens":
                continue

            f = self[col]
            # Save unnecessary copy.
            if not isinstance(f, np.ndarray):
                f = np.array(f)
            # Already good length, can skip.
            if f.shape[0] == max_seq_len:
                continue
            # Generate zero-filled primer of len=max_seq_len.
            length = len(self["seq_lens"]) * max_seq_len
            if f.dtype == np.object or f.dtype.type is np.str_:
                f_pad = [None] * length
            else:
                # Make sure type doesn't change.
                f_pad = np.zeros((length, ) + np.shape(f)[1:], dtype=f.dtype)
            # Fill primer with data.
            f_pad_base = f_base = 0
            for len_ in self["seq_lens"]:
                f_pad[f_pad_base:f_pad_base + len_] = f[f_base:f_base + len_]
                f_pad_base += max_seq_len
                f_base += len_
            assert f_base == len(f), f
            # Update our data.
            self[col] = f_pad

        # Set flags to indicate, we are now zero-padded (and to what extend).
        self.zero_padded = True
        self.max_seq_len = max_seq_len

    @PublicAPI
    def size_bytes(self) -> int:
        """
        Returns:
            int: The overall size in bytes of the data buffer (all columns).
        """
        return sum(
            v.nbytes if isinstance(v, np.ndarray) else sys.getsizeof(v)
            for v in self.values())

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    @PublicAPI
    def __getitem__(self, key: str) -> TensorType:
        """Returns one column (by key) from the data.

        Args:
            key (str): The key (column name) to return.

        Returns:
            TensorType: The data under the given key.
        """
        if not hasattr(self, key) and key in self:
            self.accessed_keys.add(key)

        # Backward compatibility for when "input-dicts" were used.
        if key == "is_training":
            if log_once("SampleBatch['is_training']"):
                deprecation_warning(
                    old="SampleBatch['is_training']",
                    new="SampleBatch.is_training",
                    error=False)
            return self.is_training

        value = dict.__getitem__(self, key)
        if self.get_interceptor is not None:
            if key not in self.intercepted_values:
                self.intercepted_values[key] = self.get_interceptor(value)
            value = self.intercepted_values[key]
        return value

    @PublicAPI
    def __setitem__(self, key, item) -> None:
        """Inserts (overrides) an entire column (by key) in the data buffer.

        Args:
            key (str): The column name to set a value for.
            item (TensorType): The data to insert.
        """
        # Defend against creating SampleBatch via pickle (no property
        # `added_keys` and first item is already set).
        if not hasattr(self, "added_keys"):
            dict.__setitem__(self, key, item)
            return

        if key not in self:
            self.added_keys.add(key)

        dict.__setitem__(self, key, item)
        if key in self.intercepted_values:
            self.intercepted_values[key] = item

    @PublicAPI
    def __delitem__(self, key):
        self.deleted_keys.add(key)
        dict.__delitem__(self, key)

    @DeveloperAPI
    def compress(self,
                 bulk: bool = False,
                 columns: Set[str] = frozenset(["obs", "new_obs"])) -> None:
        """Compresses the data buffers (by column) in place.

        Args:
            bulk (bool): Whether to compress across the batch dimension (0)
                as well. If False will compress n separate list items, where n
                is the batch size.
            columns (Set[str]): The columns to compress. Default: Only
                compress the obs and new_obs columns.
        """
        for key in columns:
            if key in self.keys():
                if bulk:
                    self[key] = pack(self[key])
                else:
                    self[key] = np.array([pack(o) for o in self[key]])

    @DeveloperAPI
    def decompress_if_needed(self,
                             columns: Set[str] = frozenset(
                                 ["obs", "new_obs"])) -> "SampleBatch":
        """Decompresses data buffers (per column if not compressed) in place.

        Args:
            columns (Set[str]): The columns to decompress. Default: Only
                decompress the obs and new_obs columns.

        Returns:
            SampleBatch: This very SampleBatch.
        """
        for key in columns:
            if key in self.keys():
                arr = self[key]
                if is_compressed(arr):
                    self[key] = unpack(arr)
                elif len(arr) > 0 and is_compressed(arr[0]):
                    self[key] = np.array([unpack(o) for o in self[key]])
        return self

    @DeveloperAPI
    def set_get_interceptor(self, fn):
        self.get_interceptor = fn

    def __repr__(self):
        return "SampleBatch({})".format(list(self.keys()))

    def _get_slice_indices(self, slice_size):
        data_slices = []
        data_slices_states = []
        if self.get("seq_lens") is not None and len(self["seq_lens"]) > 0:
            assert np.all(self["seq_lens"] < slice_size), \
                "ERROR: `slice_size` must be larger than the max. seq-len " \
                "in the batch!"
            start_pos = 0
            current_slize_size = 0
            actual_slice_idx = 0
            start_idx = 0
            idx = 0
            while idx < len(self["seq_lens"]):
                seq_len = self["seq_lens"][idx]
                current_slize_size += seq_len
                actual_slice_idx += seq_len if not self.zero_padded else \
                    self.max_seq_len
                # Complete minibatch -> Append to data_slices.
                if current_slize_size >= slice_size:
                    if not self.zero_padded:
                        data_slices.append((start_pos, start_pos + slice_size))
                        start_pos += slice_size
                        if current_slize_size > slice_size:
                            overhead = current_slize_size - slice_size
                            start_pos -= (seq_len - overhead)
                            idx -= 1
                    else:
                        data_slices.append((start_pos, actual_slice_idx))
                        start_pos = actual_slice_idx

                    data_slices_states.append((start_idx, idx + 1))
                    current_slize_size = 0
                    start_idx = idx + 1
                idx += 1
        else:
            i = 0
            while i < self.count:
                data_slices.append((i, i + slice_size))
                i += slice_size
        return data_slices, data_slices_states

    # TODO: deprecate
    @property
    def data(self):
        if log_once("SampleBatch.data"):
            deprecation_warning(
                old="SampleBatch.data[..]", new="SampleBatch[..]", error=False)
        return self

    # TODO: (sven) Experimental method.
    def get_single_step_input_dict(self, view_requirements, index="last"):
        """Creates single ts SampleBatch at given index from `self`.

        For usage as input-dict for model calls.

        Args:
            sample_batch (SampleBatch): A single-trajectory SampleBatch object
                to generate the compute_actions input dict from.
            index (Union[int, str]): An integer index value indicating the
                position in the trajectory for which to generate the
                compute_actions input dict. Set to "last" to generate the dict
                at the very end of the trajectory (e.g. for value estimation).
                Note that "last" is different from -1, as "last" will use the
                final NEXT_OBS as observation input.

        Returns:
            SampleBatch: The (single-timestep) input dict for ModelV2 calls.
        """
        last_mappings = {
            SampleBatch.OBS: SampleBatch.NEXT_OBS,
            SampleBatch.PREV_ACTIONS: SampleBatch.ACTIONS,
            SampleBatch.PREV_REWARDS: SampleBatch.REWARDS,
        }

        input_dict = {}
        for view_col, view_req in view_requirements.items():
            # Create batches of size 1 (single-agent input-dict).
            data_col = view_req.data_col or view_col
            if index == "last":
                data_col = last_mappings.get(data_col, data_col)
                # Range needed.
                if view_req.shift_from is not None:
                    data = self[view_col][-1]
                    traj_len = len(self[data_col])
                    missing_at_end = traj_len % view_req.batch_repeat_value
                    obs_shift = -1 if data_col in [
                        SampleBatch.OBS, SampleBatch.NEXT_OBS
                    ] else 0
                    from_ = view_req.shift_from + obs_shift
                    to_ = view_req.shift_to + obs_shift + 1
                    if to_ == 0:
                        to_ = None
                    input_dict[view_col] = np.array([
                        np.concatenate(
                            [data,
                             self[data_col][-missing_at_end:]])[from_:to_]
                    ])
                # Single index.
                else:
                    data = self[data_col][-1]
                    input_dict[view_col] = np.array([data])
            else:
                # Index range.
                if isinstance(index, tuple):
                    data = self[data_col][index[0]:index[1] +
                                          1 if index[1] != -1 else None]
                    input_dict[view_col] = np.array([data])
                # Single index.
                else:
                    input_dict[view_col] = self[data_col][
                        index:index + 1 if index != -1 else None]

        return SampleBatch(input_dict, seq_lens=np.array([1], dtype=np.int32))


@PublicAPI
class MultiAgentBatch:
    """A batch of experiences from multiple agents in the environment.

    Attributes:
        policy_batches (Dict[PolicyID, SampleBatch]): Mapping from policy
            ids to SampleBatches of experiences.
        count (int): The number of env steps in this batch.
    """

    @PublicAPI
    def __init__(self, policy_batches: Dict[PolicyID, SampleBatch],
                 env_steps: int):
        """Initialize a MultiAgentBatch object.

        Args:
            policy_batches (Dict[PolicyID, SampleBatch]): Mapping from policy
                ids to SampleBatches of experiences.
            env_steps (int): The number of environment steps in the environment
                this batch contains. This will be less than the number of
                transitions this batch contains across all policies in total.
        """

        for v in policy_batches.values():
            assert isinstance(v, SampleBatch)
        self.policy_batches = policy_batches
        # Called "count" for uniformity with SampleBatch.
        # Prefer to access this via the `env_steps()` method when possible
        # for clarity.
        self.count = env_steps

    @PublicAPI
    def env_steps(self) -> int:
        """The number of env steps (there are >= 1 agent steps per env step).

        Returns:
            int: The number of environment steps contained in this batch.
        """
        return self.count

    @PublicAPI
    def agent_steps(self) -> int:
        """The number of agent steps (there are >= 1 agent steps per env step).

        Returns:
            int: The number of agent steps total in this batch.
        """
        ct = 0
        for batch in self.policy_batches.values():
            ct += batch.count
        return ct

    @PublicAPI
    def timeslices(self, k: int) -> List["MultiAgentBatch"]:
        """Returns k-step batches holding data for each agent at those steps.

        For examples, suppose we have agent1 observations [a1t1, a1t2, a1t3],
        for agent2, [a2t1, a2t3], and for agent3, [a3t3] only.

        Calling timeslices(1) would return three MultiAgentBatches containing
        [a1t1, a2t1], [a1t2], and [a1t3, a2t3, a3t3].

        Calling timeslices(2) would return two MultiAgentBatches containing
        [a1t1, a1t2, a2t1], and [a1t3, a2t3, a3t3].

        This method is used to implement "lockstep" replay mode. Note that this
        method does not guarantee each batch contains only data from a single
        unroll. Batches might contain data from multiple different envs.
        """
        from ray.rllib.evaluation.sample_batch_builder import \
            SampleBatchBuilder

        # Build a sorted set of (eps_id, t, policy_id, data...)
        steps = []
        for policy_id, batch in self.policy_batches.items():
            for row in batch.rows():
                steps.append((row[SampleBatch.EPS_ID], row["t"],
                              row["agent_index"], policy_id, row))
        steps.sort()

        finished_slices = []
        cur_slice = collections.defaultdict(SampleBatchBuilder)
        cur_slice_size = 0

        def finish_slice():
            nonlocal cur_slice_size
            assert cur_slice_size > 0
            batch = MultiAgentBatch(
                {k: v.build_and_reset()
                 for k, v in cur_slice.items()}, cur_slice_size)
            cur_slice_size = 0
            finished_slices.append(batch)

        # For each unique env timestep.
        for _, group in itertools.groupby(steps, lambda x: x[:2]):
            # Accumulate into the current slice.
            for _, _, _, policy_id, row in group:
                cur_slice[policy_id].add_values(**row)
            cur_slice_size += 1
            # Slice has reached target number of env steps.
            if cur_slice_size >= k:
                finish_slice()
                assert cur_slice_size == 0

        if cur_slice_size > 0:
            finish_slice()

        assert len(finished_slices) > 0, finished_slices
        return finished_slices

    @staticmethod
    @PublicAPI
    def wrap_as_needed(
            policy_batches: Dict[PolicyID, SampleBatch],
            env_steps: int) -> Union[SampleBatch, "MultiAgentBatch"]:
        """Returns SampleBatch or MultiAgentBatch, depending on given policies.

        Args:
            policy_batches (Dict[PolicyID, SampleBatch]): Mapping from policy
                ids to SampleBatch.
            env_steps (int): Number of env steps in the batch.

        Returns:
            Union[SampleBatch, MultiAgentBatch]: The single default policy's
                SampleBatch or a MultiAgentBatch (more than one policy).
        """
        if len(policy_batches) == 1 and DEFAULT_POLICY_ID in policy_batches:
            return policy_batches[DEFAULT_POLICY_ID]
        return MultiAgentBatch(
            policy_batches=policy_batches, env_steps=env_steps)

    @staticmethod
    @PublicAPI
    def concat_samples(samples: List["MultiAgentBatch"]) -> "MultiAgentBatch":
        """Concatenates a list of MultiAgentBatches into a new MultiAgentBatch.

        Args:
            samples (List[MultiAgentBatch]): List of MultiagentBatch objects
                to concatenate.

        Returns:
            MultiAgentBatch: A new MultiAgentBatch consisting of the
                concatenated inputs.
        """
        policy_batches = collections.defaultdict(list)
        env_steps = 0
        for s in samples:
            if not isinstance(s, MultiAgentBatch):
                raise ValueError(
                    "`MultiAgentBatch.concat_samples()` can only concat "
                    "MultiAgentBatch types, not {}!".format(type(s).__name__))
            for key, batch in s.policy_batches.items():
                policy_batches[key].append(batch)
            env_steps += s.env_steps()
        out = {}
        for key, batches in policy_batches.items():
            out[key] = SampleBatch.concat_samples(batches)
        return MultiAgentBatch(out, env_steps)

    @PublicAPI
    def copy(self) -> "MultiAgentBatch":
        """Deep-copies self into a new MultiAgentBatch.

        Returns:
            MultiAgentBatch: The copy of self with deep-copied data.
        """
        return MultiAgentBatch(
            {k: v.copy()
             for (k, v) in self.policy_batches.items()}, self.count)

    @PublicAPI
    def size_bytes(self) -> int:
        """
        Returns:
            int: The overall size in bytes of all policy batches (all columns).
        """
        return sum(b.size_bytes() for b in self.policy_batches.values())

    @DeveloperAPI
    def compress(self,
                 bulk: bool = False,
                 columns: Set[str] = frozenset(["obs", "new_obs"])) -> None:
        """Compresses each policy batch (per column) in place.

        Args:
            bulk (bool): Whether to compress across the batch dimension (0)
                as well. If False will compress n separate list items, where n
                is the batch size.
            columns (Set[str]): Set of column names to compress.
        """
        for batch in self.policy_batches.values():
            batch.compress(bulk=bulk, columns=columns)

    @DeveloperAPI
    def decompress_if_needed(self,
                             columns: Set[str] = frozenset(
                                 ["obs", "new_obs"])) -> "MultiAgentBatch":
        """Decompresses each policy batch (per column), if already compressed.

        Args:
            columns (Set[str]): Set of column names to decompress.

        Returns:
            MultiAgentBatch: This very MultiAgentBatch.
        """
        for batch in self.policy_batches.values():
            batch.decompress_if_needed(columns)
        return self

    def __str__(self):
        return "MultiAgentBatch({}, env_steps={})".format(
            str(self.policy_batches), self.count)

    def __repr__(self):
        return "MultiAgentBatch({}, env_steps={})".format(
            str(self.policy_batches), self.count)
