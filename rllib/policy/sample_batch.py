import collections
import numpy as np

from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI
from ray.rllib.utils.compression import pack, unpack, is_compressed
from ray.rllib.utils.memory import concat_aligned

# Default policy id for single agent environments
DEFAULT_POLICY_ID = "default_policy"


@PublicAPI
class SampleBatch:
    """Wrapper around a dictionary with string keys and array-like values.

    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    # Outputs from interacting with the environment
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

    # Uniquely identifies an episode
    EPS_ID = "eps_id"

    # Uniquely identifies a sample batch. This is important to distinguish RNN
    # sequences from the same episode when multiple sample batches are
    # concatenated (fusing sequences across batches can be unsafe).
    UNROLL_ID = "unroll_id"

    # Uniquely identifies an agent within an episode
    AGENT_INDEX = "agent_index"

    # Value function predictions emitted by the behaviour policy
    VF_PREDS = "vf_preds"

    @PublicAPI
    def __init__(self, *args, **kwargs):
        """Constructs a sample batch (same params as dict constructor)."""

        self.data = dict(*args, **kwargs)
        lengths = []
        for k, v in self.data.copy().items():
            assert isinstance(k, str), self
            lengths.append(len(v))
            self.data[k] = np.array(v, copy=False)
        if not lengths:
            raise ValueError("Empty sample batch")
        assert len(set(lengths)) == 1, ("data columns must be same length",
                                        self.data, lengths)
        self.count = lengths[0]

    @staticmethod
    @PublicAPI
    def concat_samples(samples):
        """Concatenates n data dicts or MultiAgentBatches.

        Args:
            samples (List[Dict[np.ndarray]]]): List of dicts of data (numpy).

        Returns:
            Union[SampleBatch,MultiAgentBatch]: A new (compressed) SampleBatch/
                MultiAgentBatch.
        """
        if isinstance(samples[0], MultiAgentBatch):
            return MultiAgentBatch.concat_samples(samples)
        out = {}
        samples = [s for s in samples if s.count > 0]
        for k in samples[0].keys():
            out[k] = concat_aligned([s[k] for s in samples])
        return SampleBatch(out)

    @PublicAPI
    def concat(self, other):
        """Returns a new SampleBatch with each data column concatenated.

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
    def copy(self):
        return SampleBatch(
            {k: np.array(v, copy=True)
             for (k, v) in self.data.items()})

    @PublicAPI
    def rows(self):
        """Returns an iterator over data rows, i.e. dicts with column values.

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
    def columns(self, keys):
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
    def shuffle(self):
        """Shuffles the rows of this batch in-place."""

        permutation = np.random.permutation(self.count)
        for key, val in self.items():
            self[key] = val[permutation]

    @PublicAPI
    def split_by_episode(self):
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
    def slice(self, start, end):
        """Returns a slice of the row data of this batch.

        Args:
            start (int): Starting index.
            end (int): Ending index.

        Returns:
            SampleBatch which has a slice of this batch's data.
        """

        return SampleBatch({k: v[start:end] for k, v in self.data.items()})

    @PublicAPI
    def keys(self):
        return self.data.keys()

    @PublicAPI
    def items(self):
        return self.data.items()

    @PublicAPI
    def get(self, key):
        return self.data.get(key)

    @PublicAPI
    def __getitem__(self, key):
        return self.data[key]

    @PublicAPI
    def __setitem__(self, key, item):
        self.data[key] = item

    @DeveloperAPI
    def compress(self, bulk=False, columns=frozenset(["obs", "new_obs"])):
        for key in columns:
            if key in self.data:
                if bulk:
                    self.data[key] = pack(self.data[key])
                else:
                    self.data[key] = np.array(
                        [pack(o) for o in self.data[key]])

    @DeveloperAPI
    def decompress_if_needed(self, columns=frozenset(["obs", "new_obs"])):
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
    """A batch of experiences from multiple policies in the environment.
    """

    @PublicAPI
    def __init__(self, policy_batches, count):
        """Initializes a MultiAgentBatch object.

        Args:
            policy_batches (Dict[str,SampleBatch]): Mapping from policy id
                (str) to a SampleBatch of experiences. Note that these batches
                may be of different length.
            count (int): The number of timesteps in the environment this batch
                contains. This will be less than the number of transitions this
                batch contains across all policies in total.
        """
        self.policy_batches = policy_batches
        self.count = count

    @staticmethod
    @PublicAPI
    def wrap_as_needed(batches, count):
        """Returns SampleBatch or MultiAgentBatch, depending on given policies.

        Args:
            batches (Dict[str,SampleBatch]): Mapping from policy ID to
                SampleBatch.
            count (int): A count to use, when returning a MultiAgentBatch.

        Returns:
            Union[SampleBatch,MultiAgentBatch]: The single default policy's
                SampleBatch or a MultiAgentBatch (more than one policy).
        """
        if len(batches) == 1 and DEFAULT_POLICY_ID in batches:
            return batches[DEFAULT_POLICY_ID]
        return MultiAgentBatch(batches, count)

    @staticmethod
    @PublicAPI
    def concat_samples(samples):
        """Concatenates a list of MultiAgentBatches into a new MultiAgentBatch.

        Args:
            samples (List[MultiAgentBatch]): List of MultiagentBatch objects
                to concatenate.

        Returns:
            MultiAgentBatch: A new MultiAgentBatch consisting of the
                concatenated inputs.
        """
        policy_batches = collections.defaultdict(list)
        total_count = 0
        for s in samples:
            if not isinstance(s, MultiAgentBatch):
                raise ValueError(
                    "`MultiAgentBatch.concat_samples()` can only concat "
                    "MultiAgentBatch types, not {}!".format(type(s).__name__))
            for policy_id, batch in s.policy_batches.items():
                policy_batches[policy_id].append(batch)
            total_count += s.count
        out = {}
        for policy_id, batches in policy_batches.items():
            out[policy_id] = SampleBatch.concat_samples(batches)
        return MultiAgentBatch(out, total_count)

    @PublicAPI
    def copy(self):
        """Deep-copies self into a new MultiAgentBatch.

        Returns:
            MultiAgentBatch: The copy of self with deep-copied data.
        """
        return MultiAgentBatch(
            {k: v.copy()
             for (k, v) in self.policy_batches.items()}, self.count)

    @PublicAPI
    def total(self):
        """Calculates the sum of all step-counts over all policy batches.

        Returns:
            int: The sum of counts over all policy batches.
        """
        ct = 0
        for batch in self.policy_batches.values():
            ct += batch.count
        return ct

    @DeveloperAPI
    def compress(self, bulk=False, columns=frozenset(["obs", "new_obs"])):
        """Compresses each policy batch.

        Args:
            bulk (bool): Whether to compress across the batch dimension (0)
                as well. If False will compress n separate list items, where n
                is the batch size.
            columns (Set[str]): Set of column names to compress.
        """
        for batch in self.policy_batches.values():
            batch.compress(bulk=bulk, columns=columns)

    @DeveloperAPI
    def decompress_if_needed(self, columns=frozenset(["obs", "new_obs"])):
        """Decompresses each policy batch, if already compressed.

        Args:
            columns (Set[str]): Set of column names to decompress.
        """
        for batch in self.policy_batches.values():
            batch.decompress_if_needed(columns)
        return self

    def __str__(self):
        return "MultiAgentBatch({}, count={})".format(
            str(self.policy_batches), self.count)

    def __repr__(self):
        return "MultiAgentBatch({}, count={})".format(
            str(self.policy_batches), self.count)
