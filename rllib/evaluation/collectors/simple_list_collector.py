import collections
from gymnasium.spaces import Space
import logging
import numpy as np
import tree  # pip install dm_tree
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING, Union

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.collectors.sample_collector import SampleCollector
from ray.rllib.evaluation.collectors.agent_collector import AgentCollector
from ray.rllib.evaluation.episode import Episode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, concat_samples
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.spaces.space_utils import get_dummy_batch_for_space
from ray.rllib.utils.typing import (
    AgentID,
    EpisodeID,
    EnvID,
    PolicyID,
    TensorType,
    ViewRequirementsDict,
)
from ray.util.debug import log_once

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()

if TYPE_CHECKING:
    from ray.rllib.algorithms.callbacks import DefaultCallbacks

logger = logging.getLogger(__name__)


class _PolicyCollector:
    """Collects already postprocessed (single agent) samples for one policy.

    Samples come in through already postprocessed SampleBatches, which
    contain single episode/trajectory data for a single agent and are then
    appended to this policy's buffers.
    """

    def __init__(self, policy: Policy):
        """Initializes a _PolicyCollector instance.

        Args:
            policy: The policy object.
        """

        self.batches = []
        self.policy = policy
        # The total timestep count for all agents that use this policy.
        # NOTE: This is not an env-step count (across n agents). AgentA and
        # agentB, both using this policy, acting in the same episode and both
        # doing n steps would increase the count by 2*n.
        self.agent_steps = 0

    def add_postprocessed_batch_for_training(
        self, batch: SampleBatch, view_requirements: ViewRequirementsDict
    ) -> None:
        """Adds a postprocessed SampleBatch (single agent) to our buffers.

        Args:
            batch: An individual agent's (one trajectory)
                SampleBatch to be added to the Policy's buffers.
            view_requirements: The view
                requirements for the policy. This is so we know, whether a
                view-column needs to be copied at all (not needed for
                training).
        """
        # Add the agent's trajectory length to our count.
        self.agent_steps += batch.count
        # And remove columns not needed for training.
        for view_col, view_req in view_requirements.items():
            if view_col in batch and not view_req.used_for_training:
                del batch[view_col]
        self.batches.append(batch)

    def build(self):
        """Builds a SampleBatch for this policy from the collected data.

        Also resets all buffers for further sample collection for this policy.

        Returns:
            SampleBatch: The SampleBatch with all thus-far collected data for
                this policy.
        """
        # Create batch from our buffers.
        batch = concat_samples(self.batches)
        # Clear batches for future samples.
        self.batches = []
        # Reset agent steps to 0.
        self.agent_steps = 0
        # Add num_grad_updates counter to the policy's batch.
        batch.num_grad_updates = self.policy.num_grad_updates

        return batch


class _PolicyCollectorGroup:
    def __init__(self, policy_map):
        self.policy_collectors = {}
        # Total env-steps (1 env-step=up to N agents stepped).
        self.env_steps = 0
        # Total agent steps (1 agent-step=1 individual agent (out of N)
        # stepped).
        self.agent_steps = 0


@PublicAPI
class SimpleListCollector(SampleCollector):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(
        self,
        policy_map: PolicyMap,
        clip_rewards: Union[bool, float],
        callbacks: "DefaultCallbacks",
        multiple_episodes_in_batch: bool = True,
        rollout_fragment_length: int = 200,
        count_steps_by: str = "env_steps",
    ):
        """Initializes a SimpleListCollector instance."""

        super().__init__(
            policy_map,
            clip_rewards,
            callbacks,
            multiple_episodes_in_batch,
            rollout_fragment_length,
            count_steps_by,
        )

        self.large_batch_threshold: int = (
            max(1000, self.rollout_fragment_length * 10)
            if self.rollout_fragment_length != float("inf")
            else 5000
        )

        # Whenever we observe a new episode+agent, add a new
        # _SingleTrajectoryCollector.
        self.agent_collectors: Dict[Tuple[EpisodeID, AgentID], AgentCollector] = {}
        # Internal agent-key-to-policy-id map.
        self.agent_key_to_policy_id = {}
        # Pool of used/unused PolicyCollectorGroups (attached to episodes for
        # across-episode multi-agent sample collection).
        self.policy_collector_groups = []

        # Agents to collect data from for the next forward pass (per policy).
        self.forward_pass_agent_keys = {pid: [] for pid in self.policy_map.keys()}
        self.forward_pass_size = {pid: 0 for pid in self.policy_map.keys()}

        # Maps episode ID to the (non-built) env steps taken in this episode.
        self.episode_steps: Dict[EpisodeID, int] = collections.defaultdict(int)
        # Maps episode ID to the (non-built) individual agent steps in this
        # episode.
        self.agent_steps: Dict[EpisodeID, int] = collections.defaultdict(int)
        # Maps episode ID to Episode.
        self.episodes: Dict[EpisodeID, Episode] = {}

    @override(SampleCollector)
    def episode_step(self, episode: Episode) -> None:
        episode_id = episode.episode_id
        # In the rase case that an "empty" step is taken at the beginning of
        # the episode (none of the agents has an observation in the obs-dict
        # and thus does not take an action), we have seen the episode before
        # and have to add it here to our registry.
        if episode_id not in self.episodes:
            self.episodes[episode_id] = episode
        else:
            assert episode is self.episodes[episode_id]
        self.episode_steps[episode_id] += 1
        episode.length += 1

        # In case of "empty" env steps (no agent is stepping), the builder
        # object may still be None.
        if episode.batch_builder:
            env_steps = episode.batch_builder.env_steps
            num_individual_observations = sum(
                c.agent_steps for c in episode.batch_builder.policy_collectors.values()
            )

            if num_individual_observations > self.large_batch_threshold and log_once(
                "large_batch_warning"
            ):
                logger.warning(
                    "More than {} observations in {} env steps for "
                    "episode {} ".format(
                        num_individual_observations, env_steps, episode_id
                    )
                    + "are buffered in the sampler. If this is more than you "
                    "expected, check that that you set a horizon on your "
                    "environment correctly and that it terminates at some "
                    "point. Note: In multi-agent environments, "
                    "`rollout_fragment_length` sets the batch size based on "
                    "(across-agents) environment steps, not the steps of "
                    "individual agents, which can result in unexpectedly "
                    "large batches."
                    + (
                        "Also, you may be waiting for your Env to "
                        "terminate (batch_mode=`complete_episodes`). Make sure "
                        "it does at some point."
                        if not self.multiple_episodes_in_batch
                        else ""
                    )
                )

    @override(SampleCollector)
    def add_init_obs(
        self,
        *,
        episode: Episode,
        agent_id: AgentID,
        env_id: EnvID,
        policy_id: PolicyID,
        init_obs: TensorType,
        init_infos: Optional[Dict[str, TensorType]] = None,
        t: int = -1,
    ) -> None:
        # Make sure our mappings are up to date.
        agent_key = (episode.episode_id, agent_id)
        self.agent_key_to_policy_id[agent_key] = policy_id
        policy = self.policy_map[policy_id]

        # Add initial obs to Trajectory.
        assert agent_key not in self.agent_collectors
        # TODO: determine exact shift-before based on the view-req shifts.

        # get max_seq_len value (Default is 1)
        try:
            max_seq_len = policy.config["model"]["max_seq_len"]
        except KeyError:
            max_seq_len = 1

        self.agent_collectors[agent_key] = AgentCollector(
            policy.view_requirements,
            max_seq_len=max_seq_len,
            disable_action_flattening=policy.config.get(
                "_disable_action_flattening", False
            ),
            intial_states=policy.get_initial_state(),
            is_policy_recurrent=policy.is_recurrent(),
        )
        self.agent_collectors[agent_key].add_init_obs(
            episode_id=episode.episode_id,
            agent_index=episode._agent_index(agent_id),
            env_id=env_id,
            init_obs=init_obs,
            init_infos=init_infos or {},
            t=t,
        )

        self.episodes[episode.episode_id] = episode
        if episode.batch_builder is None:
            episode.batch_builder = (
                self.policy_collector_groups.pop()
                if self.policy_collector_groups
                else _PolicyCollectorGroup(self.policy_map)
            )

        self._add_to_next_inference_call(agent_key)

    @override(SampleCollector)
    def add_action_reward_next_obs(
        self,
        episode_id: EpisodeID,
        agent_id: AgentID,
        env_id: EnvID,
        policy_id: PolicyID,
        agent_done: bool,
        values: Dict[str, TensorType],
    ) -> None:
        # Make sure, episode/agent already has some (at least init) data.
        agent_key = (episode_id, agent_id)
        assert self.agent_key_to_policy_id[agent_key] == policy_id
        assert agent_key in self.agent_collectors

        self.agent_steps[episode_id] += 1

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.agent_collectors[agent_key].add_action_reward_next_obs(values)

        if not agent_done:
            self._add_to_next_inference_call(agent_key)

    @override(SampleCollector)
    def total_env_steps(self) -> int:
        # Add the non-built ongoing-episode env steps + the already built
        # env-steps.
        return sum(self.episode_steps.values()) + sum(
            pg.env_steps for pg in self.policy_collector_groups.values()
        )

    @override(SampleCollector)
    def total_agent_steps(self) -> int:
        # Add the non-built ongoing-episode agent steps (still in the agent
        # collectors) + the already built agent steps.
        return sum(a.agent_steps for a in self.agent_collectors.values()) + sum(
            pg.agent_steps for pg in self.policy_collector_groups.values()
        )

    @override(SampleCollector)
    def get_inference_input_dict(self, policy_id: PolicyID) -> Dict[str, TensorType]:
        policy = self.policy_map[policy_id]
        keys = self.forward_pass_agent_keys[policy_id]
        batch_size = len(keys)

        # Return empty batch, if no forward pass to do.
        if batch_size == 0:
            return SampleBatch()

        buffers = {}
        for k in keys:
            collector = self.agent_collectors[k]
            buffers[k] = collector.buffers
        # Use one agent's buffer_structs (they should all be the same).
        buffer_structs = self.agent_collectors[keys[0]].buffer_structs

        input_dict = {}
        for view_col, view_req in policy.view_requirements.items():
            # Not used for action computations.
            if not view_req.used_for_compute_actions:
                continue

            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            delta = (
                -1
                if data_col
                in [
                    SampleBatch.OBS,
                    SampleBatch.INFOS,
                    SampleBatch.ENV_ID,
                    SampleBatch.EPS_ID,
                    SampleBatch.AGENT_INDEX,
                    SampleBatch.T,
                ]
                else 0
            )
            # Range of shifts, e.g. "-100:0". Note: This includes index 0!
            if view_req.shift_from is not None:
                time_indices = (view_req.shift_from + delta, view_req.shift_to + delta)
            # Single shift (e.g. -1) or list of shifts, e.g. [-4, -1, 0].
            else:
                time_indices = view_req.shift + delta

            # Loop through agents and add up their data (batch).
            data = None
            for k in keys:
                # Buffer for the data does not exist yet: Create dummy
                # (zero) data.
                if data_col not in buffers[k]:
                    if view_req.data_col is not None:
                        space = policy.view_requirements[view_req.data_col].space
                    else:
                        space = view_req.space

                    if isinstance(space, Space):
                        fill_value = get_dummy_batch_for_space(
                            space,
                            batch_size=0,
                        )
                    else:
                        fill_value = space

                    self.agent_collectors[k]._build_buffers({data_col: fill_value})

                if data is None:
                    data = [[] for _ in range(len(buffers[keys[0]][data_col]))]

                # `shift_from` and `shift_to` are defined: User wants a
                # view with some time-range.
                if isinstance(time_indices, tuple):
                    # `shift_to` == -1: Until the end (including(!) the
                    # last item).
                    if time_indices[1] == -1:
                        for d, b in zip(data, buffers[k][data_col]):
                            d.append(b[time_indices[0] :])
                    # `shift_to` != -1: "Normal" range.
                    else:
                        for d, b in zip(data, buffers[k][data_col]):
                            d.append(b[time_indices[0] : time_indices[1] + 1])
                # Single index.
                else:
                    for d, b in zip(data, buffers[k][data_col]):
                        d.append(b[time_indices])

            np_data = [np.array(d) for d in data]
            if data_col in buffer_structs:
                input_dict[view_col] = tree.unflatten_as(
                    buffer_structs[data_col], np_data
                )
            else:
                input_dict[view_col] = np_data[0]

        self._reset_inference_calls(policy_id)

        return SampleBatch(
            input_dict,
            seq_lens=np.ones(batch_size, dtype=np.int32)
            if "state_in_0" in input_dict
            else None,
        )

    @override(SampleCollector)
    def postprocess_episode(
        self,
        episode: Episode,
        is_done: bool = False,
        check_dones: bool = False,
        build: bool = False,
    ) -> Union[None, SampleBatch, MultiAgentBatch]:
        episode_id = episode.episode_id
        policy_collector_group = episode.batch_builder

        # TODO: (sven) Once we implement multi-agent communication channels,
        #  we have to resolve the restriction of only sending other agent
        #  batches from the same policy to the postprocess methods.
        # Build SampleBatches for the given episode.
        pre_batches = {}
        for (eps_id, agent_id), collector in self.agent_collectors.items():
            # Build only if there is data and agent is part of given episode.
            if collector.agent_steps == 0 or eps_id != episode_id:
                continue
            pid = self.agent_key_to_policy_id[(eps_id, agent_id)]
            policy = self.policy_map[pid]
            pre_batch = collector.build_for_training(policy.view_requirements)
            pre_batches[agent_id] = (policy, pre_batch)

        # Apply reward clipping before calling postprocessing functions.
        if self.clip_rewards is True:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.sign(pre_batch["rewards"])
        elif self.clip_rewards:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.clip(
                    pre_batch["rewards"],
                    a_min=-self.clip_rewards,
                    a_max=self.clip_rewards,
                )

        post_batches = {}
        for agent_id, (_, pre_batch) in pre_batches.items():
            # Entire episode is said to be done.
            # Error if no DONE at end of this agent's trajectory.
            if is_done and check_dones and not pre_batch.is_terminated_or_truncated():
                raise ValueError(
                    "Episode {} terminated for all agents, but we still "
                    "don't have a last observation for agent {} (policy "
                    "{}). ".format(
                        episode_id,
                        agent_id,
                        self.agent_key_to_policy_id[(episode_id, agent_id)],
                    )
                    + "Please ensure that you include the last observations "
                    "of all live agents when setting truncated[__all__] or "
                    "terminated[__all__] to True."
                )

            # Skip a trajectory's postprocessing (and thus using it for training),
            # if its agent's info exists and contains the training_enabled=False
            # setting (used by our PolicyClients).
            last_info = episode.last_info_for(agent_id)
            if last_info and not last_info.get("training_enabled", True):
                if is_done:
                    agent_key = (episode_id, agent_id)
                    del self.agent_key_to_policy_id[agent_key]
                    del self.agent_collectors[agent_key]
                continue

            if len(pre_batches) > 1:
                other_batches = pre_batches.copy()
                del other_batches[agent_id]
            else:
                other_batches = {}
            pid = self.agent_key_to_policy_id[(episode_id, agent_id)]
            policy = self.policy_map[pid]
            if not pre_batch.is_single_trajectory():
                raise ValueError(
                    "Batches sent to postprocessing must be from a single trajectory! "
                    "TERMINATED & TRUNCATED need to be False everywhere, except the "
                    "last timestep, which can be either True or False for those keys)!",
                    pre_batch,
                )
            elif len(set(pre_batch[SampleBatch.EPS_ID])) > 1:
                episode_ids = set(pre_batch[SampleBatch.EPS_ID])
                raise ValueError(
                    "Batches sent to postprocessing must only contain steps "
                    "from a single episode! Your trajectory contains data from "
                    f"{len(episode_ids)} episodes ({list(episode_ids)}).",
                    pre_batch,
                )
            # Call the Policy's Exploration's postprocess method.
            post_batches[agent_id] = pre_batch
            if getattr(policy, "exploration", None) is not None:
                policy.exploration.postprocess_trajectory(
                    policy, post_batches[agent_id], policy.get_session()
                )
            post_batches[agent_id].set_get_interceptor(None)
            post_batches[agent_id] = policy.postprocess_trajectory(
                post_batches[agent_id], other_batches, episode
            )

        if log_once("after_post"):
            logger.info(
                "Trajectory fragment after postprocess_trajectory():\n\n{}\n".format(
                    summarize(post_batches)
                )
            )

        # Append into policy batches and reset.
        from ray.rllib.evaluation.rollout_worker import get_global_worker

        for agent_id, post_batch in sorted(post_batches.items()):
            agent_key = (episode_id, agent_id)
            pid = self.agent_key_to_policy_id[agent_key]
            policy = self.policy_map[pid]
            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=episode,
                agent_id=agent_id,
                policy_id=pid,
                policies=self.policy_map,
                postprocessed_batch=post_batch,
                original_batches=pre_batches,
            )

            # Add the postprocessed SampleBatch to the policy collectors for
            # training.
            # PID may be a newly added policy. Just confirm we have it in our
            # policy map before proceeding with adding a new _PolicyCollector()
            # to the group.
            if pid not in policy_collector_group.policy_collectors:
                assert pid in self.policy_map
                policy_collector_group.policy_collectors[pid] = _PolicyCollector(policy)
            policy_collector_group.policy_collectors[
                pid
            ].add_postprocessed_batch_for_training(post_batch, policy.view_requirements)

            if is_done:
                del self.agent_key_to_policy_id[agent_key]
                del self.agent_collectors[agent_key]

        if policy_collector_group:
            env_steps = self.episode_steps[episode_id]
            policy_collector_group.env_steps += env_steps
            agent_steps = self.agent_steps[episode_id]
            policy_collector_group.agent_steps += agent_steps

        if is_done:
            del self.episode_steps[episode_id]
            del self.episodes[episode_id]

            if episode_id in self.agent_steps:
                del self.agent_steps[episode_id]
            else:
                assert (
                    len(pre_batches) == 0
                ), "Expected the batch to be empty since the episode_id is missing."
                # if the key does not exist it means that throughout the episode all
                # observations were empty (i.e. there was no agent in the env)
                msg = (
                    f"Data from episode {episode_id} does not show any agent "
                    f"interactions. Hint: Make sure for at least one timestep in the "
                    f"episode, env.step() returns non-empty values."
                )
                raise ValueError(msg)

            # Make PolicyCollectorGroup available for more agent batches in
            # other episodes. Do not reset count to 0.
            if policy_collector_group:
                self.policy_collector_groups.append(policy_collector_group)
        else:
            self.episode_steps[episode_id] = self.agent_steps[episode_id] = 0

        # Build a MultiAgentBatch from the episode and return.
        if build:
            return self._build_multi_agent_batch(episode)

    def _build_multi_agent_batch(
        self, episode: Episode
    ) -> Union[MultiAgentBatch, SampleBatch]:

        ma_batch = {}
        for pid, collector in episode.batch_builder.policy_collectors.items():
            if collector.agent_steps > 0:
                ma_batch[pid] = collector.build()

        # TODO(sven): We should always return the same type here (MultiAgentBatch),
        #  no matter what. Just have to unify our `training_step` methods, then. This
        #  will reduce a lot of confusion about what comes out of the sampling process.
        # Create the batch.
        ma_batch = MultiAgentBatch.wrap_as_needed(
            ma_batch, env_steps=episode.batch_builder.env_steps
        )

        # PolicyCollectorGroup is empty.
        episode.batch_builder.env_steps = 0
        episode.batch_builder.agent_steps = 0

        return ma_batch

    @override(SampleCollector)
    def try_build_truncated_episode_multi_agent_batch(
        self,
    ) -> List[Union[MultiAgentBatch, SampleBatch]]:
        batches = []
        # Loop through ongoing episodes and see whether their length plus
        # what's already in the policy collectors reaches the fragment-len
        # (abiding to the unit used: env-steps or agent-steps).
        for episode_id, episode in self.episodes.items():
            # Measure batch size in env-steps.
            if self.count_steps_by == "env_steps":
                built_steps = (
                    episode.batch_builder.env_steps if episode.batch_builder else 0
                )
                ongoing_steps = self.episode_steps[episode_id]
            # Measure batch-size in agent-steps.
            else:
                built_steps = (
                    episode.batch_builder.agent_steps if episode.batch_builder else 0
                )
                ongoing_steps = self.agent_steps[episode_id]

            # Reached the fragment-len -> We should build an MA-Batch.
            if built_steps + ongoing_steps >= self.rollout_fragment_length:
                if self.count_steps_by == "env_steps":
                    assert built_steps + ongoing_steps == self.rollout_fragment_length
                # If we reached the fragment-len only because of `episode_id`
                # (still ongoing) -> postprocess `episode_id` first.
                if built_steps < self.rollout_fragment_length:
                    self.postprocess_episode(episode, is_done=False)
                # If there is a builder for this episode,
                # build the MA-batch and add to return values.
                if episode.batch_builder:
                    batch = self._build_multi_agent_batch(episode=episode)
                    batches.append(batch)
                # No batch-builder:
                # We have reached the rollout-fragment length w/o any agent
                # steps! Warn that the environment may never request any
                # actions from any agents.
                elif log_once("no_agent_steps"):
                    logger.warning(
                        "Your environment seems to be stepping w/o ever "
                        "emitting agent observations (agents are never "
                        "requested to act)!"
                    )

        return batches

    def _add_to_next_inference_call(self, agent_key: Tuple[EpisodeID, AgentID]) -> None:
        """Adds an Agent key (episode+agent IDs) to the next inference call.

        This makes sure that the agent's current data (in the trajectory) is
        used for generating the next input_dict for a
        `Policy.compute_actions()` call.

        Args:
            agent_key (Tuple[EpisodeID, AgentID]: A unique agent key (across
                vectorized environments).
        """
        pid = self.agent_key_to_policy_id[agent_key]

        # PID may be a newly added policy (added on the fly during training).
        # Just confirm we have it in our policy map before proceeding with
        # forward_pass_size=0.
        if pid not in self.forward_pass_size:
            assert pid in self.policy_map
            self.forward_pass_size[pid] = 0
            self.forward_pass_agent_keys[pid] = []

        idx = self.forward_pass_size[pid]
        assert idx >= 0
        if idx == 0:
            self.forward_pass_agent_keys[pid].clear()

        self.forward_pass_agent_keys[pid].append(agent_key)
        self.forward_pass_size[pid] += 1

    def _reset_inference_calls(self, policy_id: PolicyID) -> None:
        """Resets internal inference input-dict registries.

        Calling `self.get_inference_input_dict()` after this method is called
        would return an empty input-dict.

        Args:
            policy_id: The policy ID for which to reset the
                inference pointers.
        """
        self.forward_pass_size[policy_id] = 0
