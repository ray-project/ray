import collections
import logging
import numpy as np
from typing import List, Dict, TYPE_CHECKING, Union

from ray.rllib.evaluation.collectors.simple_list_collector import \
    _AgentCollector, _SimpleListCollector, to_float_np_array
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.typing import PolicyID
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.rllib.agents.callbacks import DefaultCallbacks

logger = logging.getLogger(__name__)


class _PreAllocAgentCollector(_AgentCollector):
    """Same as AgentCollector, but copies directly into a prealloc buffer.

    This is useful if for inference a large consecutive section on the time
    axis is needed, for example in attention nets.

    Collects samples for one agent in one trajectory (episode).
    The agent may be part of a multi-agent environment. Samples are stored in
    lists including some possible automatic "shift" buffer at the beginning to
    be able to save memory when storing things like NEXT_OBS, PREV_REWARDS,
    etc.., which are specified using the trajectory view API.
    """

    def __init__(self, shift_before: int = 0):
        super().__init__(shift_before)
        # Create pre-allocated buffers.
        self.buffers = TODO

    @override(_AgentCollector)
    def build(self, view_requirements: Dict[str, ViewRequirement]) -> \
            SampleBatch:
        """Builds a SampleBatch from the thus-far collected agent data.

        If the episode/trajectory has no DONE=True at the end, will copy
        the necessary n timesteps at the end of the trajectory back to the
        beginning of the buffers and wait for new samples coming in.
        SampleBatches created by this method will be ready for postprocessing
        by a Policy.

        Args:
            view_requirements (Dict[str, ViewRequirement]: The view
                requirements dict needed to build the SampleBatch from the raw
                buffers (which may have data shifts as well as mappings from
                view-col to data-col in them).
        Returns:
            SampleBatch: The built SampleBatch for this agent, ready to go into
                postprocessing.
        """

        # TODO: measure performance gains when using a UsageTrackingDict
        #  instead of a SampleBatch for postprocessing (this would eliminate
        #  copies (for creating this SampleBatch) of many unused columns for
        #  no reason (not used by postprocessor)).

        batch_data = {}
        np_data = {}
        for view_col, view_req in view_requirements.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            # Some columns don't exist yet (get created during postprocessing).
            # -> skip.
            if data_col not in self.buffers:
                continue
            shift = view_req.shift - \
                (1 if data_col == SampleBatch.OBS else 0)
            if data_col not in np_data:
                np_data[data_col] = to_float_np_array(self.buffers[data_col])
            if shift == 0:
                batch_data[view_col] = np_data[data_col][self.shift_before:]
            else:
                batch_data[view_col] = np_data[data_col][self.shift_before +
                                                         shift:shift]
        batch = SampleBatch(batch_data)

        if SampleBatch.UNROLL_ID not in batch.data:
            batch.data[SampleBatch.UNROLL_ID] = np.repeat(
                _AgentCollector._next_unroll_id, batch.count)
            _AgentCollector._next_unroll_id += 1

        # This trajectory is continuing -> Copy data at the end (in the size of
        # self.shift_before) to the beginning of buffers and erase everything
        # else.
        if not self.buffers[SampleBatch.DONES][-1]:
            # Copy data to beginning of buffer and cut lists.
            if self.shift_before > 0:
                for k, data in self.buffers.items():
                    self.buffers[k] = data[-self.shift_before:]
            self.count = 0

        return batch


class _PreAllocPolicyCollector:
    """Collects already postprocessed (single agent) samples for one policy.

    Holds pre-allocated numpy/torch buffers for direct insertion of data coming
    from postprocessed SampleBatches, which contain single episode/trajectory
    data for a single agent.
    """

    def __init__(self):
        """Initializes a _PolicyCollector instance."""

        self.buffers: Dict[str, List] = collections.defaultdict(list)
        # The total timestep count for all agents that use this policy.
        # NOTE: This is not an env-step count (across n agents). AgentA and
        # agentB, both using this policy, acting in the same episode and both
        # doing n steps would increase the count by 2*n.
        self.count = 0

    def add_postprocessed_batch_for_training(
            self, batch: SampleBatch,
            view_requirements: Dict[str, ViewRequirement]) -> None:
        """Adds a postprocessed SampleBatch (single agent) to our buffers.

        Args:
            batch (SampleBatch): A single agent (one trajectory) SampleBatch
                to be added to the Policy's buffers.
            view_requirements (Dict[str, ViewRequirement]: The view
                requirements for the policy. This is so we know, whether a
                view-column needs to be copied at all (not needed for
                training).
        """
        for view_col, data in batch.items():
            # Skip columns that are not used for training.
            if view_col in view_requirements and \
                    not view_requirements[view_col].used_for_training:
                continue
            self.buffers[view_col].extend(data)
        # Add the agent's trajectory length to our count.
        self.count += batch.count

    def build(self):
        """Builds a SampleBatch for this policy from the collected data.

        Also resets all buffers for further sample collection for this policy.

        Returns:
            SampleBatch: The SampleBatch with all thus-far collected data for
                this policy.
        """
        # Create batch from our buffers.
        batch = SampleBatch(self.buffers)
        assert SampleBatch.UNROLL_ID in batch.data
        # Clear buffers for future samples.
        self.buffers.clear()
        # Reset count to 0.
        self.count = 0
        return batch


class _PreAllocBufferCollector(_SimpleListCollector):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(self,
                 policy_map: Dict[PolicyID, Policy],
                 clip_rewards: Union[bool, float],
                 callbacks: "DefaultCallbacks",
                 multiple_episodes_in_batch: bool = True,
                 rollout_fragment_length: int = 200):
        """Initializes a _SimpleListCollector instance.

        Args:
            policy_map (Dict[str, Policy]): Maps policy ids to policy
                instances.
            clip_rewards (Union[bool, float]): Whether to clip rewards before
                postprocessing (at +/-1.0) or the actual value to +/- clip.
            callbacks (DefaultCallbacks): RLlib callbacks.
            multiple_episodes_in_batch (bool): Whether to pack multiple
                episodes into each batch. This guarantees batches will be
                exactly `rollout_fragment_length` in size.
            rollout_fragment_length (int): The length of a fragment to collect
                before building a SampleBatch from the data and resetting.
        """

        super().__init__(
            policy_map, clip_rewards, callbacks, multiple_episodes_in_batch,
            rollout_fragment_length)

        # Build each Policies' single collector. We "override" the super's
        # Policy collectors (which as simple list based) with the pre-alloc
        # ones.
        self.policy_collectors = {
            pid: _PreAllocPolicyCollector()
            for pid in policy_map.keys()
        }

    @override(_SimpleListCollector)
    def postprocess_episode(self,
                            episode: MultiAgentEpisode,
                            is_done: bool = False,
                            check_dones: bool = False) -> None:
        # TODO: (sven) Once we implement multi-agent communication channels,
        #  we have to resolve the restriction of only sending other agent
        #  batches from the same policy to the postprocess methods.
        # Build SampleBatches for the given episode.
        pre_batches = {}
        for (episode_id, agent_id), collector in self.agent_collectors.items():
            # Build only the given episode.
            if episode_id != episode.episode_id:
                continue
            policy = self.policy_map[self.agent_to_policy[agent_id]]
            pre_batches[(episode_id,
                         agent_id)] = (policy,
                                       collector.build(
                                           policy.view_requirements))

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

        for (episode_id, agent_id), (_, pre_batch) in pre_batches.items():
            # Entire episode is said to be done.
            if is_done:
                # Error if no DONE at end of this agent's trajectory.
                if check_dones and not pre_batch[SampleBatch.DONES][-1]:
                    raise ValueError(
                        "Episode {} terminated for all agents, but we still "
                        "don't have a last observation for agent {} (policy "
                        "{}). ".format(episode_id, agent_id,
                                       self.agent_to_policy[agent_id]) +
                        "Please ensure that you include the last observations "
                        "of all live agents when setting done[__all__] to "
                        "True. Alternatively, set no_done_at_end=True to "
                        "allow this.")
            # If (only this?) agent is done, erase its buffer entirely.
            if pre_batch[SampleBatch.DONES][-1]:
                del self.agent_collectors[(episode_id, agent_id)]

            other_batches = pre_batches.copy()
            del other_batches[(episode_id, agent_id)]
            policy = self.policy_map[self.agent_to_policy[agent_id]]
            if any(pre_batch["dones"][:-1]) or len(set(
                    pre_batch["eps_id"])) > 1:
                raise ValueError(
                    "Batches sent to postprocessing must only contain steps "
                    "from a single trajectory.", pre_batch)
            # Call the Policy's Exploration's postprocess method.
            post_batches[agent_id] = pre_batch
            if getattr(policy, "exploration", None) is not None:
                policy.exploration.postprocess_trajectory(
                    policy, post_batches[agent_id],
                    getattr(policy, "_sess", None))
            post_batches[agent_id] = policy.postprocess_trajectory(
                post_batches[agent_id], other_batches, episode)

        if log_once("after_post"):
            logger.info(
                "Trajectory fragment after postprocess_trajectory():\n\n{}\n".
                format(summarize(post_batches)))

        # Append into policy batches and reset.
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
            # Add the postprocessed SampleBatch to the policy collectors for
            # training.
            self.policy_collectors[self.agent_to_policy[
                agent_id]].add_postprocessed_batch_for_training(
                    post_batch, policy.view_requirements)

        env_steps = self.episode_steps[episode.episode_id]
        self.policy_collectors_env_steps += env_steps

        if is_done:
            del self.episode_steps[episode.episode_id]
            del self.episodes[episode.episode_id]
        else:
            self.episode_steps[episode.episode_id] = 0
