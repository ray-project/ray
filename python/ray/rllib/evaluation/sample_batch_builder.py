from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np

from ray.rllib.evaluation.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI


def to_float_array(v):
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


@PublicAPI
class SampleBatchBuilder(object):
    """Util to build a SampleBatch incrementally.

    For efficiency, SampleBatches hold values in column form (as arrays).
    However, it is useful to add data one row (dict) at a time.
    """

    @PublicAPI
    def __init__(self):
        self.buffers = collections.defaultdict(list)
        self.count = 0

    @PublicAPI
    def add_values(self, **values):
        """Add the given dictionary (row) of values to this batch."""

        for k, v in values.items():
            self.buffers[k].append(v)
        self.count += 1

    @PublicAPI
    def add_batch(self, batch):
        """Add the given batch of values to this batch."""

        for k, column in batch.items():
            self.buffers[k].extend(column)
        self.count += batch.count

    @PublicAPI
    def build_and_reset(self):
        """Returns a sample batch including all previously added values."""

        batch = SampleBatch(
            {k: to_float_array(v)
             for k, v in self.buffers.items()})
        self.buffers.clear()
        self.count = 0
        return batch


@DeveloperAPI
class MultiAgentSampleBatchBuilder(object):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(self, policy_map, clip_rewards):
        """Initialize a MultiAgentSampleBatchBuilder.

        Arguments:
            policy_map (dict): Maps policy ids to policy graph instances.
            clip_rewards (bool): Whether to clip rewards before postprocessing.
        """

        self.policy_map = policy_map
        self.clip_rewards = clip_rewards
        self.policy_builders = {
            k: SampleBatchBuilder()
            for k in policy_map.keys()
        }
        self.agent_builders = {}
        self.agent_to_policy = {}
        self.count = 0  # increment this manually

    def total(self):
        """Returns summed number of steps across all agent buffers."""

        return sum(p.count for p in self.policy_builders.values())

    def has_pending_data(self):
        """Returns whether there is pending unprocessed data."""

        return len(self.agent_builders) > 0

    @DeveloperAPI
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

    def postprocess_batch_so_far(self, episode):
        """Apply policy postprocessors to any unprocessed rows.

        This pushes the postprocessed per-agent batches onto the per-policy
        builders, clearing per-agent state.

        Arguments:
            episode: current MultiAgentEpisode object or None
        """

        # Materialize the batches so far
        pre_batches = {}
        for agent_id, builder in self.agent_builders.items():
            pre_batches[agent_id] = (
                self.policy_map[self.agent_to_policy[agent_id]],
                builder.build_and_reset())

        # Apply postprocessor
        post_batches = {}
        if self.clip_rewards:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.sign(pre_batch["rewards"])
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
                pre_batch, other_batches, episode)

        # Append into policy batches and reset
        for agent_id, post_batch in sorted(post_batches.items()):
            self.policy_builders[self.agent_to_policy[agent_id]].add_batch(
                post_batch)
        self.agent_builders.clear()
        self.agent_to_policy.clear()

    def check_missing_dones(self):
        for agent_id, builder in self.agent_builders.items():
            if builder.buffers["dones"][-1] is not True:
                raise ValueError(
                    "The environment terminated for all agents, but we still "
                    "don't have a last observation for "
                    "agent {} (policy {}). ".format(
                        agent_id, self.agent_to_policy[agent_id]) +
                    "Please ensure that you include the last observations "
                    "of all live agents when setting '__all__' done to True.")

    @DeveloperAPI
    def build_and_reset(self, episode):
        """Returns the accumulated sample batches for each policy.

        Any unprocessed rows will be first postprocessed with a policy
        postprocessor. The internal state of this builder will be reset.

        Arguments:
            episode: current MultiAgentEpisode object or None
        """

        self.postprocess_batch_so_far(episode)
        policy_batches = {}
        for policy_id, builder in self.policy_builders.items():
            if builder.count > 0:
                policy_batches[policy_id] = builder.build_and_reset()
        old_count = self.count
        self.count = 0
        return MultiAgentBatch.wrap_as_needed(policy_batches, old_count)
