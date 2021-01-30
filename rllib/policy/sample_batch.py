import collections
import numpy as np
import sys
import itertools
from typing import Dict, Iterable, List, Optional, Set, Union

from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI
from ray.rllib.utils.compression import pack, unpack, is_compressed
from ray.rllib.utils.memory import concat_aligned
from ray.rllib.utils.typing import PolicyID, TensorType

# Default policy id for single agent environments
DEFAULT_POLICY_ID = "default_policy"


@PublicAPI
class SampleBatch:
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
        self.seq_lens = kwargs.pop("_seq_lens", None)
        self.dont_check_lens = kwargs.pop("_dont_check_lens", False)
        self.max_seq_len = None
        if self.seq_lens is not None and len(self.seq_lens) > 0:
            self.max_seq_len = max(self.seq_lens)

        # The actual data, accessible by column name (str).
        self.data = dict(*args, **kwargs)

        lengths = []
        for k, v in self.data.copy().items():
            assert isinstance(k, str), self
            lengths.append(len(v))
            if isinstance(v, list):
                self.data[k] = np.array(v)
        if not lengths:
            raise ValueError("Empty sample batch")
        if not self.dont_check_lens:
            assert len(set(lengths)) == 1, \
                "Data columns must be same length, but lens are " \
                "{}".format(lengths)
        if self.seq_lens is not None and len(self.seq_lens) > 0:
            self.count = sum(self.seq_lens)
        else:
            self.count = len(next(iter(self.data.values())))

        # Keeps track of new columns added after initial ones.
        self.new_columns = []

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
        for s in samples:
            if s.count > 0:
                concat_samples.append(s)
                if s.seq_lens is not None:
                    seq_lens.extend(s.seq_lens)

        out = {}
        for k in concat_samples[0].keys():
            out[k] = concat_aligned(
                [s[k] for s in concat_samples],
                time_major=concat_samples[0].time_major)
        return SampleBatch(
            out,
            _seq_lens=np.array(seq_lens, dtype=np.int32),
            _time_major=concat_samples[0].time_major,
            _dont_check_lens=True)

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
    def copy(self) -> "SampleBatch":
        """Creates a (deep) copy of this SampleBatch and returns it.

        Returns:
            SampleBatch: A (deep) copy of this SampleBatch object.
        """
        return SampleBatch(
            {k: np.array(v, copy=True)
             for (k, v) in self.data.items()},
            _seq_lens=self.seq_lens)

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

        slices = []
        cur_eps_id = self.data["eps_id"][0]
        offset = 0
        for i in range(self.count):
            next_eps_id = self.data["eps_id"][i]
            if next_eps_id != cur_eps_id:
                slices.append(self.slice(offset, i))
                offset = i
                cur_eps_id = next_eps_id
        slices.append(self.slice(offset, self.count))
        for s in slices:
            slen = len(set(s["eps_id"]))
            assert slen == 1, (s, slen)
        assert sum(s.count for s in slices) == self.count, (slices, self.count)
        return slices

    @PublicAPI
    def slice(self, start: int, end: int) -> "SampleBatch":
        """Returns a slice of the row data of this batch (w/o copying).

        Args:
            start (int): Starting index.
            end (int): Ending index.

        Returns:
            SampleBatch: A new SampleBatch, which has a slice of this batch's
                data.
        """
        if self.seq_lens is not None and len(self.seq_lens) > 0:
            data = {k: v[start:end] for k, v in self.data.items()}
            # Fix state_in_x data.
            count = 0
            state_start = None
            seq_lens = None
            for i, seq_len in enumerate(self.seq_lens):
                count += seq_len
                if count >= end:
                    state_idx = 0
                    state_key = "state_in_{}".format(state_idx)
                    while state_key in self.data:
                        data[state_key] = self.data[state_key][state_start:i +
                                                               1]
                        state_idx += 1
                        state_key = "state_in_{}".format(state_idx)
                    seq_lens = list(self.seq_lens[state_start:i]) + [
                        seq_len - (count - end)
                    ]
                    assert sum(seq_lens) == (end - start)
                    break
                elif state_start is None and count > start:
                    state_start = i

            return SampleBatch(
                data,
                _seq_lens=np.array(seq_lens, dtype=np.int32),
                _time_major=self.time_major,
                _dont_check_lens=True)
        else:
            return SampleBatch(
                {k: v[start:end]
                 for k, v in self.data.items()},
                _seq_lens=None,
                _time_major=self.time_major)

    @PublicAPI
    def timeslices(self, k: int) -> List["SampleBatch"]:
        """Returns SampleBatches, each one representing a k-slice of this one.

        Will start from timestep 0 and produce slices of size=k.

        Args:
            k (int): The size (in timesteps) of each returned SampleBatch.

        Returns:
            List[SampleBatch]: The list of (new) SampleBatches (each one of
                size k).
        """
        out = []
        i = 0
        while i < self.count:
            out.append(self.slice(i, i + k))
            i += k
        return out

    @PublicAPI
    def keys(self) -> Iterable[str]:
        """
        Returns:
            Iterable[str]: The keys() iterable over `self.data`.
        """
        return self.data.keys()

    @PublicAPI
    def items(self) -> Iterable[TensorType]:
        """
        Returns:
            Iterable[TensorType]: The values() iterable over `self.data`.
        """
        return self.data.items()

    @PublicAPI
    def get(self, key: str) -> Optional[TensorType]:
        """Returns one column (by key) from the data or None if key not found.

        Args:
            key (str): The key (column name) to return.

        Returns:
            Optional[TensorType]: The data under the given key. None if key
                not found in data.
        """
        return self.data.get(key)

    @PublicAPI
    def size_bytes(self) -> int:
        """
        Returns:
            int: The overall size in bytes of the data buffer (all columns).
        """
        return sum(sys.getsizeof(d) for d in self.data.values())

    @PublicAPI
    def __getitem__(self, key: str) -> TensorType:
        """Returns one column (by key) from the data.

        Args:
            key (str): The key (column name) to return.

        Returns:
            TensorType: The data under the given key.
        """
        return self.data[key]

    @PublicAPI
    def __setitem__(self, key, item) -> None:
        """Inserts (overrides) an entire column (by key) in the data buffer.

        Args:
            key (str): The column name to set a value for.
            item (TensorType): The data to insert.
        """
        if key not in self.data:
            self.new_columns.append(key)
        self.data[key] = item

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
            if key in self.data:
                if bulk:
                    self.data[key] = pack(self.data[key])
                else:
                    self.data[key] = np.array(
                        [pack(o) for o in self.data[key]])

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
            if key in self.data:
                arr = self.data[key]
                if is_compressed(arr):
                    self.data[key] = unpack(arr)
                elif len(arr) > 0 and is_compressed(arr[0]):
                    self.data[key] = np.array(
                        [unpack(o) for o in self.data[key]])
        return self

    def __str__(self):
        return "SampleBatch({})".format(str(self.data))

    def __repr__(self):
        return "SampleBatch({})".format(str(self.data))

    def __iter__(self):
        return self.data.__iter__()

    def __contains__(self, x):
        return x in self.data


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
