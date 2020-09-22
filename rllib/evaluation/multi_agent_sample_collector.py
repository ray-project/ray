import logging
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING, Union

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.per_policy_sample_collector import \
    _PerPolicySampleCollector
from ray.rllib.evaluation.sample_collector import _SampleCollector
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.typing import AgentID, EnvID, EpisodeID, PolicyID, \
    TensorType
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.rllib.agents.callbacks import DefaultCallbacks

logger = logging.getLogger(__name__)


class _EpisodeRecord:
    def __init__(self, episode, status, env_steps, policy_agents):
        self.episode_obj = episode
        self.status = status
        self.env_steps = env_steps
        self.policy_agents = policy_agents


class _MultiAgentSampleCollector(_SampleCollector):
    """Builds SampleBatches for each policy (and agent) in a multi-agent env.

    Note: This is an experimental class only used when
    `config._use_trajectory_view_api` = True.
    Once `_use_trajectory_view_api` becomes the default in configs:
    This class will deprecate the `SampleBatchBuilder` class.

    Input data is collected in central per-policy buffers, which
    efficiently pre-allocate memory (over n timesteps) and re-use the same
    memory even for succeeding agents and episodes.
    Input_dicts for action computations, SampleBatches for postprocessing, and
    train_batch dicts are - if possible - created from the central per-policy
    buffers via views to avoid copying of data).
    """

    def __init__(
            self,
            policy_map: Dict[PolicyID, Policy],
            callbacks: "DefaultCallbacks",
            multiple_episodes_in_batch: bool,
            rollout_fragment_length: int,
            # TODO: (sven) make `num_agents` flexibly grow in size.
            num_agents: int = 100,
            num_timesteps=None,
            time_major: Optional[bool] = False):
        """Initializes a _MultiAgentSampleCollector object.

        Args:
            policy_map (Dict[PolicyID,Policy]): Maps policy ids to policy
                instances.
            callbacks (DefaultCallbacks): RLlib callbacks (configured in the
                Trainer config dict). Used for trajectory postprocessing event.
            multiple_episodes_in_batch (bool): Whether it's allowed to pack
                multiple episodes into one SampleBatch when building.
            num_agents (int): The max number of agent slots to pre-allocate
                in the buffer.
            num_timesteps (int): The max number of timesteps to pre-allocate
                in the buffer.
            time_major (Optional[bool]): Whether to preallocate buffers and
                collect samples in time-major fashion (TxBx...).
        """

        self.policy_map = policy_map
        self.callbacks = callbacks
        if num_agents == float("inf") or num_agents is None:
            num_agents = 1000
        self.num_agents = int(num_agents)

        # Collect SampleBatches per-policy in _PerPolicySampleCollectors.
        self.policy_sample_collectors = {}
        for pid, policy in policy_map.items():
            # Figure out max-shifts (before and after).
            view_reqs = policy.view_requirements
            max_shift_before = 0
            max_shift_after = 0
            for vr in view_reqs.values():
                shift = force_list(vr.shift)
                if max_shift_before > shift[0]:
                    max_shift_before = shift[0]
                if max_shift_after < shift[-1]:
                    max_shift_after = shift[-1]
            # Figure out num_timesteps.
            kwargs = {"time_major": time_major}
            if policy.is_recurrent():
                n = 0
                while n < rollout_fragment_length:
                    n += policy.config["model"]["max_seq_len"]
                kwargs["num_timesteps"] = n
                kwargs["time_major"] = True
            elif num_timesteps is not None:
                kwargs["num_timesteps"] = num_timesteps

            self.policy_sample_collectors[pid] = _PerPolicySampleCollector(
                num_agents=self.num_agents,
                shift_before=-max_shift_before,
                shift_after=max_shift_after,
                **kwargs)

        self.multiple_episodes_in_batch = multiple_episodes_in_batch
        self.rollout_fragment_length = rollout_fragment_length

        # Internal agent-to-policy map.
        self.agent_to_policy = {}

        # Maps episode ID to _EpisodeRecord objects.
        self.episode_registry: Dict[EpisodeID, _EpisodeRecord] = {}

        # Data columns that are added to a SampleBatch via postprocessing.
        self.data_cols_added_in_postprocessing = set()

    @override(_SampleCollector)
    def add_init_obs(self, episode: MultiAgentEpisode, agent_id: AgentID,
                     env_id: EnvID, policy_id: PolicyID,
                     obs: TensorType) -> None:
        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        episode_id = episode.episode_id

        # Keep track of our episode registry (so we know, which episodes are
        # ongoing and exactly where in the buffers).
        episode_rec = self.episode_registry.get(episode_id)
        if episode_rec is None:
            self.episode_registry[episode_id] = \
                _EpisodeRecord(episode, "ongoing", 0, {policy_id: [agent_id]})
        elif policy_id not in episode_rec.policy_agents:
            episode_rec.policy_agents[policy_id] = [agent_id]
        else:
            episode_rec.policy_agents[policy_id].append(agent_id)

        # Add initial obs to Policy-collector.
        self.policy_sample_collectors[policy_id].add_init_obs(
            episode_id, agent_id, env_id, init_obs=obs)  # chunk_num=0,

    @override(_SampleCollector)
    def add_action_reward_next_obs(self, episode_id: EpisodeID,
                                   agent_id: AgentID, env_id: EnvID,
                                   policy_id: PolicyID, agent_done: bool,
                                   values: Dict[str, TensorType]) -> None:
        assert policy_id in self.policy_sample_collectors
        episode_rec = self.episode_registry.get(episode_id)
        if episode_rec is None or episode_rec.status != "ongoing":
            raise ValueError(
                "Episode record {} does not exist!".format(episode_id))

        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.policy_sample_collectors[policy_id].add_action_reward_next_obs(
            episode_id, agent_id, env_id, agent_done, values)

    @override(_SampleCollector)
    def episode_step(self, episode_id):
        episode_rec = self.episode_registry[episode_id]
        episode_rec.env_steps += 1
        episode_rec.episode_obj.length += 1

    @override(_SampleCollector)
    def total_env_steps(self) -> int:
        return sum(
            episode_rec.env_steps if episode_rec.status == "ongoing" else 0
            for episode_rec in self.episode_registry.values())

    @override(_SampleCollector)
    def get_inference_input_dict(self, policy_id: PolicyID) -> \
            Dict[str, TensorType]:
        policy = self.policy_map[policy_id]
        view_reqs = policy.model.inference_view_requirements
        return self.policy_sample_collectors[
            policy_id].get_inference_input_dict(view_reqs)

    @override(_SampleCollector)
    def postprocess_episode(self,
                            episode: MultiAgentEpisode,
                            check_dones: bool = False,
                            cut_at_env_step: Optional[int] = None) -> None:
        # TODO: (sven) Once we implement multi-agent communication channels,
        #  we have to resolve the restriction of only sending other agent
        #  batches from the same policy to the postprocess methods.

        # Loop through each per-policy collector and create a view (for each
        # agent as SampleBatch) from its buffers for post-processing
        all_agent_batches = {}
        for pid, rc in self.policy_sample_collectors.items():
            policy = self.policy_map[pid]
            view_reqs = policy.view_requirements
            agent_batches = rc.get_per_agent_sample_batches_for_episode(
                episode.episode_id, view_reqs, cut_at_env_step=cut_at_env_step)

            for agent_key, batch in agent_batches.items():
                # Error if no DONE at end.
                if check_dones and not batch[SampleBatch.DONES][-1]:
                    raise ValueError(
                        "Episode {} terminated for all agents, but we still "
                        "don't have a last observation for agent {} (policy "
                        "{}). ".format(agent_key[0], agent_key[1], pid) +
                        "Please ensure that you include the last observations "
                        "of all live agents when setting done[__all__] to "
                        "True. Alternatively, set no_done_at_end=True to "
                        "allow this.")

                other_batches = None
                if len(agent_batches) > 1:
                    other_batches = agent_batches.copy()
                    del other_batches[agent_key]

                # Call the Policy's Exploration's postprocess method.
                if getattr(policy, "exploration", None) is not None:
                    batch = policy.exploration.postprocess_trajectory(
                        policy, batch, getattr(policy, "_sess", None))

                # Call the actual postprocessing method of the Policy, passing
                # other agents' batches (only those agents that use the same
                # policy).
                agent_batches[agent_key] = policy.postprocess_trajectory(
                    batch, other_batches, episode)

                # Add new columns' data to buffers.
                self.data_cols_added_in_postprocessing.update(
                    agent_batches[agent_key].new_columns)
                for col in self.data_cols_added_in_postprocessing:
                    data = agent_batches[agent_key].data[col]
                    rc._build_buffers({col: data[0]})
                    timesteps = data.shape[0]
                    rc.buffers[col][rc.shift_before:rc.shift_before +
                                    timesteps, rc.agent_key_to_slot[
                                        agent_key]] = data

                # TODO: (sven) What if user reassigns an existing column
                #  (e.g. rewards) to some new np-array (e.g. as is the case for
                #  Curiosity)? In this case, the postprocessing would not be
                #  captured here.

            all_agent_batches.update(agent_batches)

        # Set status of episode to postprocessed.
        self.episode_registry[episode.episode_id].status = "postprocessed"

        if log_once("after_post"):
            logger.info("Trajectory fragment after postprocess_trajectory():"
                        "\n\n{}\n".format(summarize(all_agent_batches)))

        # Handle `on_postprocess_trajectory` callback.
        from ray.rllib.evaluation.rollout_worker import get_global_worker
        for agent_key, batch in sorted(all_agent_batches.items()):
            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=episode,
                agent_id=agent_key[0],
                policy_id=self.agent_to_policy[agent_key[0]],
                policies=self.policy_map,
                postprocessed_batch=batch,
                original_batches=None)  # TODO: (sven) do we really need this?

    @override(_SampleCollector)
    def build_episode_multi_agent_batch(self, episode_id: EpisodeID) -> \
            Union[MultiAgentBatch, SampleBatch]:
        episode_rec = self.episode_registry[episode_id]
        # Make sure episode is already postprocessed.
        if not episode_rec.status != "postprocessed":
            raise ValueError(
                "Cannot call build_episode_batch on a non-postprocessed "
                "episode ({})!".format(episode_id))
        policy_batches = {}
        for policy_id, agent_ids in episode_rec.policy_agents.items():
            policy = self.policy_map[policy_id]
            view_reqs = policy.view_requirements
            rc = self.policy_sample_collectors[policy_id]
            agent_keys = [(aid, episode_id) for aid in agent_ids]
            policy_batches[policy_id] = rc.build_sample_batch_from_agent_keys(
                agent_keys, view_reqs)

        # Episode is done -> Erase it from registry.
        del self.episode_registry[episode_id]

        # Build the MultiAgentBatch and return.
        ma_batch = MultiAgentBatch.wrap_as_needed(policy_batches,
                                                  episode_rec.env_steps)
        return ma_batch

    @override(_SampleCollector)
    def try_build_truncated_episode_multi_agent_batch(self) -> \
            Union[MultiAgentBatch, SampleBatch, None]:

        # Make sure we are allowed to build SampleBatches with more than one
        # episode in them.
        assert self.multiple_episodes_in_batch is True

        # Loop through our episodes map and figure out whether to build (and
        # return a SampleBatch or not).
        consecutive_done_episodes = []
        env_steps_done_episodes = 0

        env_steps_done_episodes_consecutive = 0
        env_steps_total = 0
        possible_build_data = {}
        episode_recs_to_push_to_end = []
        episode_ids_to_erase = []
        for episode_id, episode_rec in self.episode_registry.items():
            steps = episode_rec.env_steps
            # Add to total.
            env_steps_total += steps

            # Consecutive done-episodes plus this one reach the
            # rollout_fragment_length -> Split episode_id and build a batch.
            steps_over = env_steps_done_episodes_consecutive + steps - \
                self.rollout_fragment_length
            if steps_over >= 0:
                have_to_cut = episode_rec.status == "ongoing" or steps_over > 0
                # Postprocess this episode if it's still ongoing.
                # And keep it as status=ongoing.
                if episode_rec.status == "ongoing":
                    self.postprocess_episode(
                        episode_rec.episode_obj,
                        check_dones=False,
                        cut_at_env_step=steps - steps_over)
                    episode_rec.status = "ongoing"

                agent_keys = self._get_agent_keys_by_policy(
                    consecutive_done_episodes + [episode_id])
                for pid, keys in agent_keys.items():
                    possible_build_data[pid] = self.policy_sample_collectors[
                        pid].build_sample_batch_from_agent_keys(
                            keys,
                            self.policy_map[pid].view_requirements,
                            cut_eid=episode_id if have_to_cut else None,
                            cut_at_env_step=steps - steps_over)

                # Update our episode registry.
                episode_ids_to_erase.extend(consecutive_done_episodes)
                # If episode not processed fully yet -> Keep this record, but
                # push to end of registry to maximize the chance that we will
                # have more consecutive data for future builds.
                if have_to_cut:
                    episode_rec.env_steps = steps_over
                    episode_recs_to_push_to_end.append(episode_rec)
                else:
                    episode_ids_to_erase.append(episode_id)

                # Reset consecutive done-episodes data.
                env_steps_done_episodes_consecutive = 0
                consecutive_done_episodes = []

            # Episode is done -> Try to build it (together with other
            # consecutive done ones + some still ongoing one that's large
            # enough to fill the gap till rollout_fragment_length).
            if episode_rec.status != "ongoing":
                env_steps_done_episodes += steps
                env_steps_done_episodes_consecutive += steps
                consecutive_done_episodes.append(episode_id)
            # Hit an ongoing episode -> Reset .
            else:
                # Reset consecutive done-episodes data.
                env_steps_done_episodes_consecutive = 0
                consecutive_done_episodes = []

        # Erase done and already built episodes.
        for episode_id in episode_ids_to_erase:
            del self.episode_registry[episode_id]
        # Push still-ongoing, but already (partially used) episodes to end
        # of registry.
        for episode_rec in episode_recs_to_push_to_end:
            episode_id = episode_rec.episode_obj.episode_id
            if episode_id in self.episode_registry:
                del self.episode_registry[episode_id]
                self.episode_registry[episode_id] = episode_rec

        # Something to build, return MultiAgentBatch/SampleBatch.
        if possible_build_data:
            ma_batch = MultiAgentBatch.wrap_as_needed(
                possible_build_data, self.rollout_fragment_length)
            return ma_batch

        # Return None if nothing to build.
        return

    def _get_agent_keys_by_policy(self, episode_ids: List[EpisodeID]) -> \
            Dict[PolicyID, Tuple[AgentID, EpisodeID]]:
        agent_keys = {}
        for eid in episode_ids:
            for pid, agent_ids in self.episode_registry[
                    eid].policy_agents.items():
                if pid not in agent_keys:
                    agent_keys[pid] = []
                agent_keys[pid].extend([(a, eid) for a in agent_ids])
        return agent_keys
