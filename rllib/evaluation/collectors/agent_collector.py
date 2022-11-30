import logging

from copy import deepcopy
from gym.spaces import Space
import math
import numpy as np
import tree  # pip install dm_tree
from typing import Any, Dict, List, Optional

from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.spaces.space_utils import (
    flatten_to_single_ndarray,
    get_dummy_batch_for_space,
)
from ray.rllib.utils.typing import (
    EpisodeID,
    EnvID,
    TensorType,
    ViewRequirementsDict,
)

from ray.util import log_once
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


def _to_float_np_array(v: List[Any]) -> np.ndarray:
    if torch and torch.is_tensor(v[0]):
        raise ValueError
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


@PublicAPI
class AgentCollector:
    """Collects samples for one agent in one trajectory (episode).

    The agent may be part of a multi-agent environment. Samples are stored in
    lists including some possible automatic "shift" buffer at the beginning to
    be able to save memory when storing things like NEXT_OBS, PREV_REWARDS,
    etc.., which are specified using the trajectory view API.
    """

    _next_unroll_id = 0  # disambiguates unrolls within a single episode

    # TODO: @kourosh add different types of padding. e.g. zeros vs. same
    def __init__(
        self,
        view_reqs: ViewRequirementsDict,
        *,
        max_seq_len: int = 1,
        disable_action_flattening: bool = True,
        intial_states: Optional[List[TensorType]] = None,
        is_policy_recurrent: bool = False,
        is_training: bool = True,
    ):
        """Initialize an AgentCollector.

        Args:
            view_reqs: A dict of view requirements for the agent.
            max_seq_len: The maximum sequence length to store.
            disable_action_flattening: If True, don't flatten the action.
            intial_states: The initial states from the policy.get_initial_states()
            is_policy_recurrent: If True, the policy is recurrent.
            is_training: Sets the is_training flag for the buffers. if True, all the
                timesteps are stored in the buffers until explictly build_for_training
                () is called. if False, only the content required for the last time
                step is stored in the buffers. This will save memory during inference.
                You can change the behavior at runtime by calling is_training(mode).
        """
        self.max_seq_len = max_seq_len
        self.disable_action_flattening = disable_action_flattening
        self.view_requirements = view_reqs
        self.intial_states = intial_states or []
        self.is_policy_recurrent = is_policy_recurrent
        self._is_training = is_training

        # Determine the size of the buffer we need for data before the actual
        # episode starts. This is used for 0-buffering of e.g. prev-actions,
        # or internal state inputs.
        view_req_shifts = [
            min(vr.shift_arr) - int((vr.data_col or k) == SampleBatch.OBS)
            for k, vr in view_reqs.items()
        ]
        self.shift_before = -min(view_req_shifts)

        # The actual data buffers. Keys are column names, values are lists
        # that contain the sub-components (e.g. for complex obs spaces) with
        # each sub-component holding a list of per-timestep tensors.
        # E.g.: obs-space = Dict(a=Discrete(2), b=Box((2,)))
        # buffers["obs"] = [
        #    [0, 1],  # <- 1st sub-component of observation
        #    [np.array([.2, .3]), np.array([.0, -.2])]  # <- 2nd sub-component
        # ]
        # NOTE: infos and state_out_... are not flattened due to them often
        # using custom dict values whose structure may vary from timestep to
        # timestep.
        self.buffers: Dict[str, List[List[TensorType]]] = {}
        # Maps column names to an example data item, which may be deeply
        # nested. These are used such that we'll know how to unflatten
        # the flattened data inside self.buffers when building the
        # SampleBatch.
        self.buffer_structs: Dict[str, Any] = {}
        # The episode ID for the agent for which we collect data.
        self.episode_id = None
        # The unroll ID, unique across all rollouts (within a RolloutWorker).
        self.unroll_id = None
        # The simple timestep count for this agent. Gets increased by one
        # each time a (non-initial!) observation is added.
        self.agent_steps = 0
        # Keep track of view requirements that have a view on columns that we gain from
        # inference and also need for inference. These have dummy values appended in
        # buffers to account for the missing value when building for inference
        # Example: We have one 'state_in' view requirement that has a view on our
        # state_outs at t=[-10, ..., -1]. At any given build_for_inference()-call,
        # the buffer must contain eleven values from t=[-10, ..., 0] for us to index
        # properly. Since state_out at t=0 is missing, we substitute it with a buffer
        # value that should never make it into batches built for training.
        self.data_cols_with_dummy_values = set()

    @property
    def training(self) -> bool:
        return self._is_training

    def is_training(self, is_training: bool) -> None:
        self._is_training = is_training

    def is_empty(self) -> bool:
        """Returns True if this collector has no data."""
        return not self.buffers or all(len(item) == 0 for item in self.buffers.values())

    def _check_view_requirement(self, view_requirement_name: str, data: TensorType):
        """Warns if data does not fit the view requirement.

        Should raise an AssertionError if data does not fit the view requirement in the
        future.
        """

        if view_requirement_name in self.view_requirements:
            vr = self.view_requirements[view_requirement_name]
            # We only check for the shape here, because conflicting dtypes are often
            # because of float conversion
            # TODO (Artur): Revisit test_multi_agent_env for cases where we accept a
            #  space that is not a gym.Space
            if (
                hasattr(vr.space, "shape")
                and not vr.space.shape == np.shape(data)
                and log_once(
                    f"view_requirement"
                    f"_{view_requirement_name}_checked_in_agent_collector"
                )
            ):

                # TODO (Artur): Enforce VR shape
                # TODO (Artur): Enforce dtype as well
                logger.warning(
                    f"Provided tensor\n{data}\n does not match space of view "
                    f"requirements {view_requirement_name}.\n"
                    f"Provided tensor has shape {np.shape(data)} and view requirement "
                    f"has shape shape {vr.space.shape}."
                    f"Make sure dimensions match to resolve this warning."
                )

    def add_init_obs(
        self,
        episode_id: EpisodeID,
        agent_index: int,
        env_id: EnvID,
        init_obs: TensorType,
        t: int = -1,
    ) -> None:
        """Adds an initial observation (after reset) to the Agent's trajectory.

        Args:
            episode_id: Unique ID for the episode we are adding the
                initial observation for.
            agent_index: Unique int index (starting from 0) for the agent
                within its episode. Not to be confused with AGENT_ID (Any).
            env_id: The environment index (in a vectorized setup).
            init_obs: The initial observation tensor (after
            `env.reset()`).
            t: The time step (episode length - 1). The initial obs has
                ts=-1(!), then an action/reward/next-obs at t=0, etc..
        """
        # Store episode ID + unroll ID, which will be constant throughout this
        # AgentCollector's lifecycle.
        self.episode_id = episode_id
        if self.unroll_id is None:
            self.unroll_id = AgentCollector._next_unroll_id
            AgentCollector._next_unroll_id += 1

        # convert init_obs to np.array (in case it is a list)
        if isinstance(init_obs, list):
            init_obs = np.array(init_obs)

        # Check if view requirement dict has the SampleBatch.OBS key and warn once if
        # view requirement does not match init_obs
        self._check_view_requirement(SampleBatch.OBS, init_obs)

        if SampleBatch.OBS not in self.buffers:
            single_row = {
                SampleBatch.OBS: init_obs,
                SampleBatch.AGENT_INDEX: agent_index,
                SampleBatch.ENV_ID: env_id,
                SampleBatch.T: t,
                SampleBatch.EPS_ID: self.episode_id,
                SampleBatch.UNROLL_ID: self.unroll_id,
            }

            # TODO (Artur): Remove when PREV_ACTIONS and PREV_REWARDS get deprecated.
            # Note (Artur): As long as we have these in our default view requirements,
            # we should  build buffers with neutral elements instead of building them
            # on the first AgentCollector.build_for_inference call if present.
            # This prevents us from accidentally building buffers with duplicates of
            # the first incoming value.
            if SampleBatch.PREV_REWARDS in self.view_requirements:
                single_row[SampleBatch.REWARDS] = get_dummy_batch_for_space(
                    space=self.view_requirements[SampleBatch.REWARDS].space,
                    batch_size=0,
                    fill_value=0.0,
                )
            if SampleBatch.PREV_ACTIONS in self.view_requirements:
                potentially_flattened_batch = get_dummy_batch_for_space(
                    space=self.view_requirements[SampleBatch.ACTIONS].space,
                    batch_size=0,
                    fill_value=0.0,
                )
                if not self.disable_action_flattening:
                    potentially_flattened_batch = flatten_to_single_ndarray(
                        potentially_flattened_batch
                    )
                single_row[SampleBatch.ACTIONS] = potentially_flattened_batch
            self._build_buffers(single_row)

        # Append data to existing buffers.
        flattened = tree.flatten(init_obs)
        for i, sub_obs in enumerate(flattened):
            self.buffers[SampleBatch.OBS][i].append(sub_obs)
        self.buffers[SampleBatch.AGENT_INDEX][0].append(agent_index)
        self.buffers[SampleBatch.ENV_ID][0].append(env_id)
        self.buffers[SampleBatch.T][0].append(t)
        self.buffers[SampleBatch.EPS_ID][0].append(self.episode_id)
        self.buffers[SampleBatch.UNROLL_ID][0].append(self.unroll_id)

    def add_action_reward_next_obs(self, input_values: Dict[str, TensorType]) -> None:
        """Adds the given dictionary (row) of values to the Agent's trajectory.

        Args:
            values: Data dict (interpreted as a single row) to be added to buffer.
            Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and NEXT_OBS.
        """
        if self.unroll_id is None:
            self.unroll_id = AgentCollector._next_unroll_id
            AgentCollector._next_unroll_id += 1

        # Next obs -> obs.
        # TODO @kourosh: remove the in-place operations and get rid of this deepcopy.
        values = deepcopy(input_values)
        assert SampleBatch.OBS not in values
        values[SampleBatch.OBS] = values[SampleBatch.NEXT_OBS]
        del values[SampleBatch.NEXT_OBS]

        # convert obs to np.array (in case it is a list)
        if isinstance(values[SampleBatch.OBS], list):
            values[SampleBatch.OBS] = np.array(values[SampleBatch.OBS])

        # Default to next timestep if not provided in input values
        if SampleBatch.T not in input_values:
            values[SampleBatch.T] = self.buffers[SampleBatch.T][0][-1] + 1

        # Make sure EPS_ID/UNROLL_ID stay the same for this agent.
        if SampleBatch.EPS_ID in values:
            assert values[SampleBatch.EPS_ID] == self.episode_id
            del values[SampleBatch.EPS_ID]
        self.buffers[SampleBatch.EPS_ID][0].append(self.episode_id)
        if SampleBatch.UNROLL_ID in values:
            assert values[SampleBatch.UNROLL_ID] == self.unroll_id
            del values[SampleBatch.UNROLL_ID]
        self.buffers[SampleBatch.UNROLL_ID][0].append(self.unroll_id)

        for k, v in values.items():
            # Check if view requirement dict has k and warn once if
            # view requirement does not match v
            self._check_view_requirement(k, v)

            if k not in self.buffers:
                if self.training and k.startswith("state_out_"):
                    vr = self.view_requirements[k]
                    data_col = vr.data_col or k
                    self._fill_buffer_with_initial_values(
                        data_col, vr, build_for_inference=False
                    )
                else:
                    self._build_buffers({k: v})
            # Do not flatten infos, state_out_ and (if configured) actions.
            # Infos/state-outs may be structs that change from timestep to
            # timestep.
            should_flatten_action_key = (
                k == SampleBatch.ACTIONS and not self.disable_action_flattening
            )
            if (
                k == SampleBatch.INFOS
                or k.startswith("state_out_")
                or should_flatten_action_key
            ):
                if should_flatten_action_key:
                    v = flatten_to_single_ndarray(v)
                # Briefly remove dummy value to add to buffer
                if k in self.data_cols_with_dummy_values:
                    dummy = self.buffers[k][0].pop(-1)
                self.buffers[k][0].append(v)
                # Add back dummy value
                if k in self.data_cols_with_dummy_values:
                    self.buffers[k][0].append(dummy)
            # Flatten all other columns.
            else:
                flattened = tree.flatten(v)
                for i, sub_list in enumerate(self.buffers[k]):
                    # Briefly remove dummy value to add to buffer
                    if k in self.data_cols_with_dummy_values:
                        dummy = sub_list.pop(-1)
                    sub_list.append(flattened[i])
                    # Add back dummy value
                    if k in self.data_cols_with_dummy_values:
                        sub_list.append(dummy)

        # In inference mode, we don't need to keep all of trajectory in memory
        # we only need to keep the steps required. We can pop from the beginning to
        # create room for new data.
        if not self.training:
            for k in self.buffers:
                for sub_list in self.buffers[k]:
                    if sub_list:
                        sub_list.pop(0)

        self.agent_steps += 1

    def build_for_inference(self) -> SampleBatch:
        """During inference, we will build a SampleBatch with a batch size of 1 that
        can then be used to run the forward pass of a policy. This data will only
        include the enviornment context for running the policy at the last timestep.

        Returns:
            A SampleBatch with a batch size of 1.
        """

        batch_data = {}
        np_data = {}
        for view_col, view_req in self.view_requirements.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col

            # if this view is not for inference, skip it.
            if not view_req.used_for_compute_actions:
                continue

            if np.any(view_req.shift_arr > 0):
                raise ValueError(
                    f"During inference the agent can only use past observations to "
                    f"respect causality. However, view_col = {view_col} seems to "
                    f"depend on future indices {view_req.shift_arr}, while the "
                    f"used_for_compute_actions flag is set to True. Please fix the "
                    f"discrepancy. Hint: If you are using a custom model make sure "
                    f"the view_requirements are initialized properly and is point "
                    f"only refering to past timesteps during inference."
                )

            # Some columns don't exist yet
            # (get created during postprocessing or depend on state_out).
            if data_col not in self.buffers:
                self._fill_buffer_with_initial_values(
                    data_col, view_req, build_for_inference=True
                )
                self._prepare_for_data_cols_with_dummy_values(data_col)

            # Keep an np-array cache, so we don't have to regenerate the
            # np-array for different view_cols using to the same data_col.
            self._cache_in_np(np_data, data_col)

            data = []
            for d in np_data[data_col]:
                # if shift_arr = [0] the data will be just the last time step
                # (len(d) - 1), if shift_arr = [-1] the data will be just the timestep
                # before the last one (len(d) - 2) and so on.
                element_at_t = d[view_req.shift_arr + len(d) - 1]
                if element_at_t.shape[0] == 1:
                    # squeeze to remove the T dimension if it is 1.
                    element_at_t = element_at_t.squeeze(0)
                # add the batch dimension with [None]
                data.append(element_at_t[None])

            if data:
                batch_data[view_col] = self._unflatten_as_buffer_struct(data, data_col)

        batch = self._get_sample_batch(batch_data)
        return batch

    # TODO: @kouorsh we don't really need view_requirements anymore since it's already
    # and attribute of the class
    def build_for_training(
        self, view_requirements: ViewRequirementsDict
    ) -> SampleBatch:
        """Builds a SampleBatch from the thus-far collected agent data.

        If the episode/trajectory has no DONE=True at the end, will copy
        the necessary n timesteps at the end of the trajectory back to the
        beginning of the buffers and wait for new samples coming in.
        SampleBatches created by this method will be ready for postprocessing
        by a Policy.

        Args:
            view_requirements: The viewrequirements dict needed to build the
            SampleBatch from the raw buffers (which may have data shifts as well as
            mappings from view-col to data-col in them).

        Returns:
            SampleBatch: The built SampleBatch for this agent, ready to go into
            postprocessing.
        """
        batch_data = {}
        np_data = {}
        for view_col, view_req in view_requirements.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col

            if data_col not in self.buffers:
                is_state = self._fill_buffer_with_initial_values(
                    data_col, view_req, build_for_inference=False
                )

                # We need to skip this view_col if it does not exist in the buffers and
                # is not an RNN state because it could be the special keys that gets
                # added by policy's postprocessing function for training.
                if not is_state:
                    continue

            # OBS are already shifted by -1 (the initial obs starts one ts
            # before all other data columns).
            obs_shift = -1 if data_col == SampleBatch.OBS else 0

            # Keep an np-array cache so we don't have to regenerate the
            # np-array for different view_cols using to the same data_col.
            self._cache_in_np(np_data, data_col)

            # Go through each time-step in the buffer and construct the view
            # accordingly.
            data = []
            for d in np_data[data_col]:
                shifted_data = []

                # batch_repeat_value determines how many time steps should we skip
                # before we repeat indexing the data.
                # Example: batch_repeat_value=10, shift_arr = [-3, -2, -1],
                # shift_before = 3
                # buffer = [-3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
                # resulting_data = [[-3, -2, -1], [7, 8, 9]]
                # explanation: For t=0, we output [-3, -2, -1]. We then skip 10 time
                # steps ahead and get to t=10. For t=10, we output [7, 8, 9]. We skip
                # 10 more time steps and get to t=20. but since t=20 is out of bound we
                # stop.

                # count computes the number of time steps that we need to consider.
                # if batch_repeat_value = 1, this number should be the length of
                # episode so far, which is len(buffer) - shift_before (-1 if this
                # value was gained during inference. This is because we keep a dummy
                # value at the last position of the buffer that makes it one longer).
                count = int(
                    math.ceil(
                        (
                            len(d)
                            - int(data_col in self.data_cols_with_dummy_values)
                            - self.shift_before
                        )
                        / view_req.batch_repeat_value
                    )
                )
                for i in range(count):
                    # the indices for time step t
                    inds = (
                        self.shift_before
                        + obs_shift
                        + view_req.shift_arr
                        + (i * view_req.batch_repeat_value)
                    )

                    # handle the case where the inds are out of bounds from the end.
                    # if during the indexing any of the indices are out of bounds, we
                    # need to use padding on the end to fill in the missing indices.
                    element_at_t = []
                    for index in inds:
                        if index < len(d):
                            element_at_t.append(d[index])
                        else:
                            # zero pad similar to the last element.
                            element_at_t.append(
                                tree.map_structure(np.zeros_like, d[-1])
                            )
                    element_at_t = np.stack(element_at_t)

                    if element_at_t.shape[0] == 1:
                        # squeeze to remove the T dimension if it is 1.
                        element_at_t = element_at_t.squeeze(0)
                    shifted_data.append(element_at_t)

                # in some multi-agent cases shifted_data may be an empty list.
                # In this case we should just create an empty array and return it.
                if shifted_data:
                    shifted_data_np = np.stack(shifted_data, 0)
                else:
                    shifted_data_np = np.array(shifted_data)
                data.append(shifted_data_np)

            if data:
                batch_data[view_col] = self._unflatten_as_buffer_struct(data, data_col)

        batch = self._get_sample_batch(batch_data)

        # This trajectory is continuing -> Copy data at the end (in the size of
        # self.shift_before) to the beginning of buffers and erase everything
        # else.
        if (
            SampleBatch.DONES in self.buffers
            and not self.buffers[SampleBatch.DONES][0][-1]
        ):
            # Copy data to beginning of buffer and cut lists.
            if self.shift_before > 0:
                for k, data in self.buffers.items():
                    # Loop through
                    for i in range(len(data)):
                        self.buffers[k][i] = data[i][-self.shift_before :]
            self.agent_steps = 0

        # Reset our unroll_id.
        self.unroll_id = None

        return batch

    def _build_buffers(self, single_row: Dict[str, TensorType]) -> None:
        """Builds the buffers for sample collection, given an example data row.

        Args:
            single_row (Dict[str, TensorType]): A single row (keys=column
                names) of data to base the buffers on.
        """
        for col, data in single_row.items():
            if col in self.buffers:
                continue

            shift = self.shift_before - (
                1
                if col
                in [
                    SampleBatch.OBS,
                    SampleBatch.EPS_ID,
                    SampleBatch.AGENT_INDEX,
                    SampleBatch.ENV_ID,
                    SampleBatch.T,
                    SampleBatch.UNROLL_ID,
                ]
                else 0
            )

            # Store all data as flattened lists, except INFOS and state-out
            # lists. These are monolithic items (infos is a dict that
            # should not be further split, same for state-out items, which
            # could be custom dicts as well).
            should_flatten_action_key = (
                col == SampleBatch.ACTIONS and not self.disable_action_flattening
            )
            if (
                col == SampleBatch.INFOS
                or col.startswith("state_out_")
                or should_flatten_action_key
            ):
                if should_flatten_action_key:
                    data = flatten_to_single_ndarray(data)
                self.buffers[col] = [[data for _ in range(shift)]]
            else:
                self.buffers[col] = [
                    [v for _ in range(shift)] for v in tree.flatten(data)
                ]
                # Store an example data struct so we know, how to unflatten
                # each data col.
                self.buffer_structs[col] = data

    def _get_sample_batch(self, batch_data: Dict[str, TensorType]) -> SampleBatch:
        """Returns a SampleBatch from the given data dictionary. Also updates the
        sequence information based on the max_seq_len."""

        # Due to possible batch-repeats > 1, columns in the resulting batch
        # may not all have the same batch size.
        batch = SampleBatch(batch_data, is_training=self.training)

        # Adjust the seq-lens array depending on the incoming agent sequences.
        if self.is_policy_recurrent:
            seq_lens = []
            max_seq_len = self.max_seq_len
            count = batch.count
            while count > 0:
                seq_lens.append(min(count, max_seq_len))
                count -= max_seq_len
            batch["seq_lens"] = np.array(seq_lens)
            batch.max_seq_len = max_seq_len

        return batch

    def _cache_in_np(self, cache_dict: Dict[str, List[np.ndarray]], key: str) -> None:
        """Caches the numpy version of the key in the buffer dict."""
        if key not in cache_dict:
            cache_dict[key] = [_to_float_np_array(d) for d in self.buffers[key]]

    def _unflatten_as_buffer_struct(
        self, data: List[np.ndarray], key: str
    ) -> np.ndarray:
        """Unflattens the given to match the buffer struct format for that key."""
        if key not in self.buffer_structs:
            return data[0]

        return tree.unflatten_as(self.buffer_structs[key], data)

    def _fill_buffer_with_initial_values(
        self,
        data_col: str,
        view_requirement: ViewRequirement,
        build_for_inference: bool = False,
    ) -> bool:
        """Fills the buffer with the initial values for the given data column.
        for dat_col starting with `state_out`, use the initial states of the policy,
        but for other data columns, create a dummy value based on the view requirement
        space.

        Args:
            data_col: The data column to fill the buffer with.
            view_requirement: The view requirement for the view_col. Normally the view
                requirement for the data column is used and if it does not exist for
                some reason the view requirement for view column is used instead.
            build_for_inference: Whether this is getting called for inference or not.

        returns:
            is_state: True if the data_col is an RNN state, False otherwise.
        """
        try:
            space = self.view_requirements[data_col].space
        except KeyError:
            space = view_requirement.space

        # special treatment for state_out_<i>
        # add them to the buffer in case they don't exist yet
        is_state = True
        if data_col.startswith("state_out_"):
            if not self.is_policy_recurrent:
                raise ValueError(
                    f"{data_col} is not available, because the given policy is"
                    f"not recurrent according to the input model_inital_states."
                    f"Have you forgotten to return non-empty lists in"
                    f"policy.get_initial_states()?"
                )
            state_ind = int(data_col.split("_")[-1])
            self._build_buffers({data_col: self.intial_states[state_ind]})
        else:
            is_state = False
            # only create dummy data during inference
            if build_for_inference:
                if isinstance(space, Space):
                    #  state_out_x assumes the values do not have a batch dimension
                    #  (i.e. instead of being (1, d) it is of shape (d,).
                    fill_value = get_dummy_batch_for_space(
                        space,
                        batch_size=0,
                    )
                else:
                    fill_value = space

                self._build_buffers({data_col: fill_value})

        return is_state

    def _prepare_for_data_cols_with_dummy_values(self, data_col):
        self.data_cols_with_dummy_values.add(data_col)
        # For items gained during inference, we append a dummy value here so
        # that view requirements viewing these is not shifted by 1
        for b in self.buffers[data_col]:
            b.append(b[-1])
