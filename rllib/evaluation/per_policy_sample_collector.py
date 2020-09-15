import logging
import numpy as np
from typing import Dict, Optional

from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.typing import AgentID, EnvID, EpisodeID, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


class _PerPolicySampleCollector:
    """A class for efficiently collecting samples for a single (fixed) policy.

    Can be used by a _MultiAgentSampleCollector for its different policies.
    """

    def __init__(self,
                 num_agents: Optional[int] = None,
                 num_timesteps: Optional[int] = None,
                 time_major: bool = True,
                 shift_before: int = 0,
                 shift_after: int = 0):
        """Initializes a _PerPolicySampleCollector object.

        Args:
            num_agents (int): The max number of agent slots to pre-allocate
                in the buffer.
            num_timesteps (int): The max number of timesteps to pre-allocate
                in the buffer.
            time_major (Optional[bool]): Whether to preallocate buffers and
                collect samples in time-major fashion (TxBx...).
            shift_before (int): The additional number of time slots to
                pre-allocate at the beginning of a time window (for possible
                underlying data column shifts, e.g. PREV_ACTIONS).
            shift_after (int): The additional number of time slots to
                pre-allocate at the end of a time window (for possible
                underlying data column shifts, e.g. NEXT_OBS).
        """

        self.num_agents = num_agents or 100
        self.num_timesteps = num_timesteps
        self.time_major = time_major
        # `shift_before must at least be 1 for the init obs timestep.
        self.shift_before = max(shift_before, 1)
        self.shift_after = shift_after

        # The actual underlying data-buffers.
        self.buffers = {}

        # Next agent-slot to be used by a new agent/env combination.
        self.agent_slot_cursor = 0
        # Maps agent/episode ID to an agent slot.
        self.agent_key_to_slot = {}
        # Maps agent slot number to agent keys.
        self.slot_to_agent_key = [None] * self.num_agents
        # Maps agent slot number to an env_step offset to be subtracted from
        # the "t" column to get the env_step within the current partial
        # trajectory that the agent slot represents.
        self.env_step_offsets = [0] * self.num_agents
        # Maps agent/episode ID to a time step cursor.
        self.agent_key_to_timestep = {}

        # Indices (T,B) to pick from the buffers for the next forward pass.
        self.inference_indices = [[], []]
        self.inference_single_time_index = None
        self.inference_agent_index_bounds = None
        self.inference_size = 0
        # Maps index from the forward pass batch to (agent_id, episode_id,
        # env_id) tuple.
        self.inference_index_to_agent_info = {}
        self.agent_key_to_inference_index = {}

    def add_init_obs(self, episode_id: EpisodeID, agent_id: AgentID,
                     env_id: EnvID,
                     init_obs: TensorType) -> None:
        """Adds a single initial observation (after env.reset()) to the buffer.

        Args:
            episode_id (EpisodeID): Unique ID for the episode we are adding the
                initial observation for.
            agent_id (AgentID): Unique ID for the agent we are adding the
                initial observation for.
            env_id (EnvID): The env ID to which `init_obs` belongs.
            init_obs (TensorType): Initial observation (after env.reset()).
        """
        agent_key = (agent_id, episode_id)
        agent_slot = self.agent_slot_cursor
        self.agent_key_to_slot[agent_key] = agent_slot
        self.slot_to_agent_key[agent_slot] = agent_key
        self.env_step_offsets[agent_slot] = 0
        self._next_agent_slot()

        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(
                single_row={
                    SampleBatch.OBS: init_obs,
                    SampleBatch.EPS_ID: episode_id,
                    SampleBatch.AGENT_INDEX: agent_id,
                    "t": -1,
                    "env_id": env_id,
                })
        # Add obs (and timestep at init obs (-1) for consistency).
        if self.time_major:
            self.buffers[SampleBatch.OBS][self.shift_before-1, agent_slot] = \
                init_obs
            self.buffers["t"][self.shift_before - 1, agent_slot] = -1
        else:
            self.buffers[SampleBatch.OBS][agent_slot, self.shift_before-1] = \
                init_obs
            self.buffers["t"][agent_slot, self.shift_before - 1] = -1

        self.agent_key_to_timestep[agent_key] = self.shift_before

        self._add_to_next_inference_call(agent_key, env_id, agent_slot,
                                         self.shift_before - 1)

    def add_action_reward_next_obs(
            self, episode_id: EpisodeID, agent_id: AgentID, env_id: EnvID,
            agent_done: bool, values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            episode_id (EpisodeID): Unique ID for the episode we are adding the
                values for.
            agent_id (AgentID): Unique ID for the agent we are adding the
                values for.
            env_id (EnvID): The env ID to which the given data belongs.
            agent_done (bool): Whether next obs should not be used for an
                upcoming inference call. Default: False = next-obs should be
                used for upcoming inference.
            values (Dict[str, TensorType]): Data dict (interpreted as a single
                row) to be added to buffer. Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and NEXT_OBS.
        """
        assert (SampleBatch.ACTIONS in values and SampleBatch.REWARDS in values
                and SampleBatch.NEXT_OBS in values
                and SampleBatch.DONES in values)

        assert SampleBatch.OBS not in values
        values[SampleBatch.OBS] = values[SampleBatch.NEXT_OBS]
        del values[SampleBatch.NEXT_OBS]

        agent_key = (agent_id, episode_id)#, chunk_num)
        agent_slot = self.agent_key_to_slot[agent_key]
        ts = self.agent_key_to_timestep[agent_key]
        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            if self.time_major:
                self.buffers[k][ts, agent_slot] = v
            else:
                self.buffers[k][agent_slot, ts] = v
        self.agent_key_to_timestep[agent_key] += 1

        # Check whether we have to postprocess anything or build a SampleBatch.
        # Postprocessing:

        # Time-axis is "full" -> Cut-over to new chunk (only if not DONE).
        if self.agent_key_to_timestep[agent_key] - self.shift_before > self.num_timesteps:
                #not values[SampleBatch.DONES]:
            #TODO (sven): Rollout_fragment_length must have been hit -> Build a
                # SampleBatch and return it. Mark episode as DONE or point
                # _EpisodeRecord to new chunk.
            raise NotImplementedError()
            #self._new_chunk_from(agent_slot, agent_key,
            #                     self.agent_key_to_timestep[agent_key])

        if agent_done:
            del self.agent_key_to_inference_index[agent_key]
        else:
            self._add_to_next_inference_call(agent_key, env_id, agent_slot, ts)

        #TODO: (sven) put warning on settings here in case buffers get very
        # large.
        # if (_sample_collector.total_env_steps() > large_batch_threshold
        #        and log_once("large_batch_warning")):
        #    logger.warning(
        #        "More than {} observations for {} env steps ".format(
        #            _sample_collector.total_env_steps(),
        #            _sample_collector.count) +
        #        "are buffered in the sampler. If this is more than you "
        #        "expected, check that that you set a horizon on your "
        #        "environment correctly and that it terminates at some point. "
        #        "Note: In multi-agent environments, `rollout_fragment_length` "
        #        "sets the batch size based on (across-agents) environment "
        #        "steps, not the steps of individual agents, which can result "
        #        "in unexpectedly large batches." +
        #        ("Also, you may be in evaluation waiting for your Env to "
        #         "terminate (batch_mode=`complete_episodes`). Make sure it "
        #         "does at some point."
        #         if not multiple_episodes_in_batch else ""))

    def get_inference_input_dict(self, view_reqs: Dict[str, ViewRequirement]
                                 ) -> Dict[str, TensorType]:
        """Returns an input_dict for an (inference) forward pass.

        The input_dict can then be used for action computations inside a
        Policy via `Policy.compute_actions_from_input_dict()`.

        Args:
            view_reqs (Dict[str, ViewRequirement]): The view requirements
                dict to use.

        Returns:
            Dict[str, TensorType]: The input_dict to be passed into the ModelV2
                for inference/training.

        Throws:
            ValueError: In case no inference data has been registered, since
                the last call to this method, via calling
                `self.add_action_reward_next_obs()` with data.

        Examples:
            >>> obs, r, done, info = env.step(action)
            >>> collector.add_action_reward_next_obs(12345, 0, "pol0", {
            ...     "action": action, "obs": obs, "reward": r, "done": done
            ... })
            >>> input_dict = collector.get_inference_input_dict(policy.model)
            >>> action = policy.compute_actions_from_input_dict(input_dict)
            >>> # repeat
        """
        if self.inference_size == 0:
            raise ValueError(
                "Cannot do inference w/o registered data! Call "
                "`add_action_reward_next_obs` one or more times to register "
                "some inference data.")

        input_dict = {}
        for view_col, view_req in view_reqs.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            if data_col not in self.buffers:
                self._build_buffers({data_col: view_req.space.sample()})

            # Fast way: We only have one time slot and a consecutive range of
            # agent slots -> No need to copy.
            if self.inference_single_time_index is not None:
                t = self.inference_single_time_index
                a0, a1 = self.inference_agent_index_bounds
                if self.time_major:
                    input_dict[view_col] = self.buffers[data_col][t, a0:a1 + 1]
                else:
                    if isinstance(view_req.shift, (list, tuple)):
                        # TODO: (sven)
                        raise NotImplementedError()
                    else:
                        input_dict[view_col] = \
                            self.buffers[data_col][a0:a1 + 1, t]
            # Collect the (scattered) indices registered for the next inference
            # call.
            else:
                indices = self.inference_indices
                if self.time_major:
                    input_dict[view_col] = self.buffers[data_col][indices]
                else:
                    if isinstance(view_req.shift, (list, tuple)):
                        time_indices = \
                            np.array(view_req.shift) + np.array(indices[0])
                        input_dict[view_col] = self.buffers[data_col][indices[1],
                                                                      time_indices]
                    else:
                        input_dict[view_col] = \
                            self.buffers[data_col][indices[1], indices[0]]

        self._reset_inference_call()

        return input_dict

    def get_per_agent_sample_batches_for_episode(
            self,
            episode_id: EpisodeID,
            view_reqs: Dict[str, ViewRequirement],
            cut_at_env_step: Optional[int] = None) -> \
            Dict[AgentID, SampleBatch]:
        """Returns a dict of AgentID to SampleBatch mappings for some episode.

        Args:
            episode_id (EpisodeID): The MultiAgentEpisode ID to
                get the to-be-postprocessed SampleBatches for.
            view_reqs (Dict[str, ViewRequirement]): The view requirements dict
                to use for creating the SampleBatch from our buffers.
            cut_at_env_step (Optional[int]): An optional env_step cutoff. If
                provided, we will cut each agents' trajectory after this many
                across-all-agents env-steps (not actual time steps!) and move
                the piece beyond `cut_at_env_step` to a new agent_slot and
                only return the piece up to `cut_at_env_step`.

        Returns:
            Dict[AgentID, SampleBatch]: The sample batch objects to be passed
                to `Policy.postprocess_trajectory()`.
        """
        # Loop through all agents and create a SampleBatch
        # (as "view"; no copying).

        # Construct the SampleBatch-dict.
        sample_batch_data = {}

        for agent_key, agent_slot in self.agent_key_to_slot.items():
            if agent_key[1] != episode_id:
                continue
            end = self.agent_key_to_timestep[agent_key]
            env_step_offset = self.env_step_offsets[agent_slot]
            if cut_at_env_step is not None and self.buffers["t"][end - 1, agent_slot] - env_step_offset >= cut_at_env_step:
                try:
                    end -= next(i for i, t in enumerate(reversed(self.buffers["t"][:end, agent_slot])) if t - env_step_offset < cut_at_env_step)
                except Exception as e:
                    print("") #TODO
                    raise e
            # Do not build any empty SampleBatches.
            if end == self.shift_before:
                continue

            assert agent_key not in sample_batch_data
            sample_batch_data[agent_key] = {}
            batch = sample_batch_data[agent_key]

            for view_col, view_req in view_reqs.items():
                data_col = view_req.data_col or view_col
                # Skip columns that will only get added through postprocessing
                # (these may not even exist yet).
                if data_col not in self.buffers:
                    continue

                shift = view_req.shift
                if data_col == SampleBatch.OBS:
                    shift -= 1

                batch[view_col] = self.buffers[data_col][
                    self.shift_before + shift:end + shift, agent_slot]

        batches = {}
        for agent_key, data in sample_batch_data.items():
            batches[agent_key] = SampleBatch(data)
        return batches

    def build_sample_batch_from_agent_keys(self, agent_keys, view_reqs, cut_eid=None, cut_at_env_step=None) -> SampleBatch:
        # Gather some stats to be able to collect the SampleBatch efficiently.
        t_start = self.shift_before
        t_end = t_start + self.num_timesteps
        seq_lens = []
        agent_slots = []
        is_consecutive = True
        for agent_key in agent_keys:
            agent_slot = self.agent_key_to_slot.get(agent_key)
            if agent_slot is None:
                continue
            if len(agent_slots) > 0 and agent_slot != agent_slots[-1] + 1:
                is_consecutive = False
            # Calculate the seq_lens array.
            end = self.agent_key_to_timestep[agent_key]
            seq_len = end - self.shift_before
            erase_agent_slot = False
            # The episode needs to be cut and the second half copied into a new
            # agent_slot.
            if agent_key[1] == cut_eid:
                env_step_offset = self.env_step_offsets[agent_slot]
                env_steps = self.buffers["t"][
                                end - 1, agent_slot] - env_step_offset
                # This agent has already had more env_steps than the cut ->
                # Need to copy the remaining trajectory over to a new agent
                # slot.
                if env_steps > cut_at_env_step:
                    # Find the time slot, at which to cut.
                    try:
                        seq_len -= next(i for i, t in enumerate(reversed(self.buffers["t"][:end, agent_slot])) if t - env_step_offset < cut_at_env_step)
                    except Exception as e:
                        print("") #TODO
                        raise e
                # Cut data over to new agent slot.
                if not self.buffers[SampleBatch.DONES][seq_len + self.shift_before - 1, agent_slot]:
                    self._copy_data_to_new_agent_slot(agent_slot, agent_key, seq_len + self.shift_before)
                else:
                    erase_agent_slot = True
            else:
                erase_agent_slot = True

            if erase_agent_slot:
                # Reset everything for new data.
                #print("deleting agent_key={} (slot={})".format(agent_key, agent_slot))
                self.slot_to_agent_key[agent_slot] = None
                del self.agent_key_to_slot[agent_key]
                del self.agent_key_to_timestep[agent_key]

            agent_slots.append(agent_slot)
            seq_lens.append(seq_len)

        # Construct the view dict.
        view = {}
        for view_col, view_req in view_reqs.items():
            data_col = view_req.data_col or view_col
            assert data_col in self.buffers
            # For OBS, indices must be shifted by -1.
            shift = view_req.shift
            shift += 0 if data_col != SampleBatch.OBS else -1
            # We don't have to copy, create a view into the buffer.
            if is_consecutive:
                view[view_col] = self.buffers[data_col][
                                 t_start + shift:t_end + shift,
                                 agent_slots[0]:agent_slots[-1] + 1]
            # We have to copy data, do a indexed select from the buffer.
            else:
                view[view_col] = self.buffers[data_col][
                                 t_start + shift:t_end + shift, agent_slots]

        batch = SampleBatch(
            view, _seq_lens=np.array(seq_lens), _time_major=self.time_major)

        return batch

    def _build_buffers(self, single_row: Dict[str, TensorType]) -> None:
        """Builds the internal data buffers based on a single given row.

        This may be called several times in the lifetime of this instance
        to add new columns to the buffer. Columns in `single_row` that already
        exist in the buffer will be ignored.

        Args:
            single_row (Dict[str, TensorType]): A single datarow with one or
                more columns (str as key, np.ndarray|tensor as data) to be used
                as template to build the pre-allocated buffer.
        """
        time_size = self.num_timesteps + self.shift_before + self.shift_after
        for col, data in single_row.items():
            if col in self.buffers:
                continue
            base_shape = (time_size, self.num_agents) if self.time_major else \
                (self.num_agents, time_size)
            # Python primitive -> np.array.
            if isinstance(data, (int, float, bool, str)):
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if t_ == int else np.bool_ \
                    if t_ == bool else np.unicode_
                self.buffers[col] = np.zeros(shape=base_shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = base_shape + data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)

    def _next_agent_slot(self):
        """Starts a new agent slot at the end of the agent-axis.

        Also makes sure, the new slot is not taken yet.
        """
        self.agent_slot_cursor += 1
        if self.agent_slot_cursor >= self.num_agents:
            self.agent_slot_cursor = 0
        # Just make sure, there is space in our buffer.
        assert self.slot_to_agent_key[self.agent_slot_cursor] is None

    def _copy_data_to_new_agent_slot(self, agent_slot, agent_key, timestep):
        """Copies data from an agent slot to a new one, starting from timestep.

        The agent's data will be copied into the next available agent slot,
        and only data after timestep will be copied.

        Args:
            agent_slot (int): The agent to copy data for.
            agent_key (Tuple[AgentID, EpisodeID]): The internal key to
                identify an active agent in some episode.
            timestep (int): The timestep in the old data from which to copy
                over.
        """
        time_range = self.agent_key_to_timestep[agent_key] - (timestep - self.shift_before)
        new_agent_slot = self.agent_slot_cursor
        #print("copying data from slot {} to {}".format(agent_slot, new_agent_slot))
        # Copy relevant timesteps at end of old chunk into new one.
        if self.time_major:
            for k in self.buffers.keys():
                self.buffers[k][:time_range, new_agent_slot] = \
                    self.buffers[k][
                    timestep - self.shift_before:self.agent_key_to_timestep[agent_key], agent_slot]
        else:
            for k in self.buffers.keys():
                self.buffers[k][new_agent_slot, 0:self.shift_before] = \
                    self.buffers[k][
                    agent_slot, timestep - self.shift_before:timestep]

        self.agent_key_to_slot[agent_key] = new_agent_slot
        self.slot_to_agent_key[agent_slot] = None
        self.slot_to_agent_key[new_agent_slot] = agent_key
        self.env_step_offsets[new_agent_slot] = self.buffers["t"][timestep - self.shift_before, agent_slot] + 1
        self._next_agent_slot()
        self.agent_key_to_timestep[agent_key] = time_range

        # Fix the inference registry.
        idx = self.agent_key_to_inference_index.get(agent_key)
        if idx is not None:
            self.inference_indices[0][idx] = time_range - 1
            self.inference_indices[1][idx] = new_agent_slot

    def _add_to_next_inference_call(self, agent_key, env_id, agent_slot,
                                    timestep):
        """Registers given T and B (agent_slot) for get_inference_input_dict.

        Calling `get_inference_input_dict` will produce an input_dict (for
        Policy.compute_actions_from_input_dict) with all registered agent/time
        indices and then automatically reset the registry.

        Args:
            agent_key (Tuple[AgentID, EpisodeID, int]): The internal key to
                identify an active agent in some episode.
            env_id (EnvID): The env ID of the given agent.
            agent_slot (int): The agent_slot to register (B axis).
            timestep (int): The timestep to register (T axis).
        """
        idx = self.inference_size
        self.inference_index_to_agent_info[idx] = (agent_key[0],
                                                      agent_key[1], env_id)
        self.agent_key_to_inference_index[agent_key] = idx
        if self.inference_size == 0:
            self.inference_indices[0].clear()
            self.inference_indices[1].clear()
            self.inference_single_time_index = timestep
            self.inference_agent_index_bounds = [agent_slot, agent_slot]
        self.inference_indices[0].append(timestep)
        self.inference_indices[1].append(agent_slot)

        # Check whether we need to invalidate the conditions for copy-less
        # inference calls (using a direct view into the buffer for the forward
        # call).
        if self.inference_size > 0:
            # We won't have a single timestep -> Set to None.
            if timestep != self.inference_single_time_index:
                self.inference_single_time_index = None
                self.inference_agent_index_bounds = None
            # We won't have a consecutive range of agent slots -> Set to None.
            elif agent_slot != self.inference_agent_index_bounds[1] + 1:
                self.inference_single_time_index = None
                self.inference_agent_index_bounds = None
            else:
                self.inference_agent_index_bounds[1] = agent_slot

        self.inference_size += 1

    def _reset_inference_call(self):
        """Resets indices for the next inference call.

        After calling this, new calls to `add_init_obs()` and
        `add_action_reward_next_obs()` will count for the next input_dict
        returned by `get_inference_input_dict()`.
        """
        self.inference_size = 0
        self.inference_single_time_index = None
        self.inference_agent_index_bounds = None
