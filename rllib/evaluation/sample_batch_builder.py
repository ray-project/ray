import collections
import logging
import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI
from ray.rllib.utils.debug import summarize
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


def to_float_array(v):
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


@PublicAPI
class SampleBatchBuilder:
    """Util to build a SampleBatch incrementally.

    For efficiency, SampleBatches hold values in column form (as arrays).
    However, it is useful to add data one row (dict) at a time.
    """

    @PublicAPI
    def __init__(self):
        self.buffers = collections.defaultdict(list)
        self.count = 0
        self.unroll_id = 0  # disambiguates unrolls within a single episode

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
        batch.data[SampleBatch.UNROLL_ID] = np.repeat(self.unroll_id,
                                                      batch.count)
        self.buffers.clear()
        self.count = 0
        self.unroll_id += 1
        return batch


@DeveloperAPI
class MultiAgentSampleBatchBuilder:
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(self, policy_map, clip_rewards, callbacks):
        """Initialize a MultiAgentSampleBatchBuilder.

        Args:
            policy_map (Dict[str,Policy]): Maps policy ids to policy instances.
            clip_rewards (Union[bool,float]): Whether to clip rewards before
                postprocessing (at +/-1.0) or the actual value to +/- clip.
            callbacks (DefaultCallbacks): RLlib callbacks.
        """

        self.policy_map = policy_map
        self.clip_rewards = clip_rewards
        # Build the Policies' SampleBatchBuilders.
        self.policy_builders = {
            k: SampleBatchBuilder()
            for k in policy_map.keys()
        }
        # Whenever we observe a new agent, add a new SampleBatchBuilder for
        # this agent.
        self.agent_builders = {}
        # Internal agent-to-policy map.
        self.agent_to_policy = {}
        self.callbacks = callbacks
        # Number of "inference" steps taken in the environment.
        # Regardless of the number of agents involved in each of these steps.
        self.count = 0

    def total(self):
        """Returns the total number of steps taken in the env (all agents).

        Returns:
            int: The number of steps taken in total in the environment over all
                agents.
        """

        return sum(a.count for a in self.agent_builders.values())

    def has_pending_agent_data(self):
        """Returns whether there is pending unprocessed data.

        Returns:
            bool: True if there is at least one per-agent builder (with data
                in it).
        """

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
        self.agent_builders[agent_id].add_values(**values)

    def postprocess_batch_so_far(self, episode=None):
        """Apply policy postprocessors to any unprocessed rows.

        This pushes the postprocessed per-agent batches onto the per-policy
        builders, clearing per-agent state.

        Args:
            episode (Optional[MultiAgentEpisode]): The Episode object that
                holds this MultiAgentBatchBuilder object.
        """

        # Materialize the batches so far.
        pre_batches = {}
        for agent_id, builder in self.agent_builders.items():
            pre_batches[agent_id] = (
                self.policy_map[self.agent_to_policy[agent_id]],
                builder.build_and_reset())

        # Apply postprocessor.
        post_batches = {}
        if self.clip_rewards is True:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.sign(pre_batch["rewards"])
        elif self.clip_rewards:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.clip(
                    pre_batch["rewards"],
                    a_min=-self.clip_rewards,
                    a_max=self.clip_rewards)
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
            # Call the Policy's Exploration's postprocess method.
            if getattr(policy, "exploration", None) is not None:
                policy.exploration.postprocess_trajectory(
                    policy, post_batches[agent_id],
                    getattr(policy, "_sess", None))

        if log_once("after_post"):
            logger.info(
                "Trajectory fragment after postprocess_trajectory():\n\n{}\n".
                format(summarize(post_batches)))

        # Append into policy batches and reset
        from ray.rllib.evaluation.rollout_worker import get_global_worker
        for agent_id, post_batch in sorted(post_batches.items()):
            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=episode,
                agent_id=agent_id,
                policy_id=self.agent_to_policy[agent_id],
                policies=self.policy_map,
                postprocessed_batch=post_batch,
                original_batches=pre_batches)
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
                    "of all live agents when setting '__all__' done to True. "
                    "Alternatively, set no_done_at_end=True to allow this.")

    @DeveloperAPI
    def build_and_reset(self, episode=None):
        """Returns the accumulated sample batches for each policy.

        Any unprocessed rows will be first postprocessed with a policy
        postprocessor. The internal state of this builder will be reset.

        Args:
            episode (Optional[MultiAgentEpisode]): The Episode object that
                holds this MultiAgentBatchBuilder object or None.

        Returns:
            MultiAgentBatch: Returns the accumulated sample batches for each
                policy.
        """

        self.postprocess_batch_so_far(episode)
        policy_batches = {}
        for policy_id, builder in self.policy_builders.items():
            if builder.count > 0:
                policy_batches[policy_id] = builder.build_and_reset()
        old_count = self.count
        self.count = 0
        return MultiAgentBatch.wrap_as_needed(policy_batches, old_count)
