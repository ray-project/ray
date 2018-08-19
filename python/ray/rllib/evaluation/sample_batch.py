from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np

# Defaults policy id for single agent environments
DEFAULT_POLICY_ID = "default"


def to_float_array(v):
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


class SampleBatchBuilder(object):
    """Util to build a SampleBatch incrementally.

    For efficiency, SampleBatches hold values in column form (as arrays).
    However, it is useful to add data one row (dict) at a time.
    """

    def __init__(self):
        self.buffers = collections.defaultdict(list)
        self.count = 0

    def add_values(self, **values):
        """Add the given dictionary (row) of values to this batch."""

        for k, v in values.items():
            self.buffers[k].append(v)
        self.count += 1

    def add_batch(self, batch):
        """Add the given batch of values to this batch."""

        for k, column in batch.items():
            self.buffers[k].extend(column)
        self.count += batch.count

    def build_and_reset(self):
        """Returns a sample batch including all previously added values."""

        batch = SampleBatch(
            {k: to_float_array(v)
             for k, v in self.buffers.items()})
        self.buffers.clear()
        self.count = 0
        return batch


class MultiAgentSampleBatchBuilder(object):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(self, policy_map):
        """Initialize a MultiAgentSampleBatchBuilder.

        Arguments:
            policy_map (dict): Maps policy ids to policy graph instances.
        """

        self.policy_map = policy_map
        self.policy_builders = {
            k: SampleBatchBuilder()
            for k in policy_map.keys()
        }
        self.agent_builders = {}
        self.agent_to_policy = {}
        self.count = 0  # increment this manually

    def has_pending_data(self):
        """Returns whether there is pending unprocessed data."""

        return len(self.agent_builders) > 0

    def add_values(self, agent_id, policy_id, **values):
        """Add the given dictionary (row) of values to this batch.

        Arguments:
            agent_id (obj): Unique id for the agent we are adding values for.
            policy_id (obj): Unique id for policy controlling the agent.
            values (dict): Row of values to add for this agent.
        """

        if agent_id not in self.agent_builders:
            self.agent_builders[agent_id] = SampleBatchBuilder()
            self.agent_to_policy[agent_id] = policy_id
        builder = self.agent_builders[agent_id]
        builder.add_values(**values)

    def postprocess_batch_so_far(self):
        """Apply policy postprocessors to any unprocessed rows.

        This pushes the postprocessed per-agent batches onto the per-policy
        builders, clearing per-agent state.
        """

        # Materialize the batches so far
        pre_batches = {}
        for agent_id, builder in self.agent_builders.items():
            pre_batches[agent_id] = (
                self.policy_map[self.agent_to_policy[agent_id]],
                builder.build_and_reset())

        # Apply postprocessor
        post_batches = {}
        for agent_id, (_, pre_batch) in pre_batches.items():
            other_batches = pre_batches.copy()
            del other_batches[agent_id]
            policy = self.policy_map[self.agent_to_policy[agent_id]]
            if any(pre_batch["dones"][:-1]) or len(set(
                    pre_batch["eps_id"])) > 1:
                raise ValueError(
                    "Batches sent to postprocessing must only contain steps "
                    "from a single trajectory.", pre_batch)
            post_batches[agent_id] = policy.postprocess_trajectory(
                pre_batch, other_batches)

        # Append into policy batches and reset
        for agent_id, post_batch in sorted(post_batches.items()):
            self.policy_builders[self.agent_to_policy[agent_id]].add_batch(
                post_batch)
        self.agent_builders.clear()
        self.agent_to_policy.clear()

    def build_and_reset(self):
        """Returns the accumulated sample batches for each policy.

        Any unprocessed rows will be first postprocessed with a policy
        postprocessor. The internal state of this builder will be reset.
        """

        self.postprocess_batch_so_far()
        policy_batches = {}
        for policy_id, builder in self.policy_builders.items():
            if builder.count > 0:
                policy_batches[policy_id] = builder.build_and_reset()
        old_count = self.count
        self.count = 0
        return MultiAgentBatch.wrap_as_needed(policy_batches, old_count)


class MultiAgentBatch(object):
    """A batch of experiences from multiple policies in the environment.

    Attributes:
        policy_batches (dict): Mapping from policy id to a normal SampleBatch
            of experiences. Note that these batches may be of different length.
        count (int): The number of timesteps in the environment this batch
            contains. This will be less than the number of transitions this
            batch contains across all policies in total.
    """

    def __init__(self, policy_batches, count):
        self.policy_batches = policy_batches
        self.count = count

    @staticmethod
    def wrap_as_needed(batches, count):
        if len(batches) == 1 and DEFAULT_POLICY_ID in batches:
            return batches[DEFAULT_POLICY_ID]
        return MultiAgentBatch(batches, count)

    @staticmethod
    def concat_samples(samples):
        policy_batches = collections.defaultdict(list)
        total_count = 0
        for s in samples:
            assert isinstance(s, MultiAgentBatch)
            for policy_id, batch in s.policy_batches.items():
                policy_batches[policy_id].append(batch)
            total_count += s.count
        out = {}
        for policy_id, batches in policy_batches.items():
            out[policy_id] = SampleBatch.concat_samples(batches)
        return MultiAgentBatch(out, total_count)

    def total(self):
        ct = 0
        for batch in self.policy_batches.values():
            ct += batch.count
        return ct

    def __str__(self):
        return "MultiAgentBatch({}, count={})".format(
            str(self.policy_batches), self.count)

    def __repr__(self):
        return "MultiAgentBatch({}, count={})".format(
            str(self.policy_batches), self.count)


class SampleBatch(object):
    """Wrapper around a dictionary with string keys and array-like values.

    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    def __init__(self, *args, **kwargs):
        """Constructs a sample batch (same params as dict constructor)."""

        self.data = dict(*args, **kwargs)
        lengths = []
        for k, v in self.data.copy().items():
            assert type(k) == str, self
            lengths.append(len(v))
        if not lengths:
            raise ValueError("Empty sample batch")
        assert len(set(lengths)) == 1, "data columns must be same length"
        self.count = lengths[0]

    @staticmethod
    def concat_samples(samples):
        if isinstance(samples[0], MultiAgentBatch):
            return MultiAgentBatch.concat_samples(samples)
        out = {}
        samples = [s for s in samples if s.count > 0]
        for k in samples[0].keys():
            out[k] = np.concatenate([s[k] for s in samples])
        return SampleBatch(out)

    def concat(self, other):
        """Returns a new SampleBatch with each data column concatenated.

        Examples:
            >>> b1 = SampleBatch({"a": [1, 2]})
            >>> b2 = SampleBatch({"a": [3, 4, 5]})
            >>> print(b1.concat(b2))
            {"a": [1, 2, 3, 4, 5]}
        """

        assert self.keys() == other.keys(), "must have same columns"
        out = {}
        for k in self.keys():
            out[k] = np.concatenate([self[k], other[k]])
        return SampleBatch(out)

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

    def columns(self, keys):
        """Returns a list of just the specified columns.

        Examples:
            >>> batch = SampleBatch({"a": [1], "b": [2], "c": [3]})
            >>> print(batch.columns(["a", "b"]))
            [[1], [2]]
        """

        out = []
        for k in keys:
            out.append(self[k])
        return out

    def shuffle(self):
        permutation = np.random.permutation(self.count)
        for key, val in self.items():
            self[key] = val[permutation]

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, item):
        self.data[key] = item

    def __str__(self):
        return "SampleBatch({})".format(str(self.data))

    def __repr__(self):
        return "SampleBatch({})".format(str(self.data))

    def keys(self):
        return self.data.keys()

    def items(self):
        return self.data.items()

    def __iter__(self):
        return self.data.__iter__()

    def __contains__(self, x):
        return x in self.data
