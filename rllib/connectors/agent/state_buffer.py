from collections import defaultdict
import logging
import pickle
from typing import Any

import numpy as np
from ray.rllib.utils.annotations import override
import tree  # dm_tree

from ray.rllib.connectors.connector import (
    AgentConnector,
    Connector,
    ConnectorContext,
)
from ray import cloudpickle
from ray.rllib.connectors.registry import register_connector
from ray.rllib.core.columns import Columns
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import ActionConnectorDataType, AgentConnectorDataType
from ray.rllib.utils.annotations import OldAPIStack


logger = logging.getLogger(__name__)


@OldAPIStack
class StateBufferConnector(AgentConnector):
    def __init__(self, ctx: ConnectorContext, states: Any = None):
        super().__init__(ctx)

        self._initial_states = ctx.initial_states
        self._action_space_struct = get_base_struct_from_space(ctx.action_space)

        self._states = defaultdict(lambda: defaultdict(lambda: (None, None, None)))
        self._enable_new_api_stack = False
        # TODO(jungong) : we would not need this if policies are never stashed
        # during the rollout of a single episode.
        if states:
            try:
                self._states = cloudpickle.loads(states)
            except pickle.UnpicklingError:
                # StateBufferConnector states are only needed for rare cases
                # like stashing then restoring a policy during the rollout of
                # a single episode.
                # It is ok to ignore the error for most of the cases here.
                logger.info(
                    "Can not restore StateBufferConnector states. This warning can "
                    "usually be ignore, unless it is from restoring a stashed policy."
                )

    @override(Connector)
    def in_eval(self):
        super().in_eval()

    def reset(self, env_id: str):
        # States should not be carried over between episodes.
        if env_id in self._states:
            del self._states[env_id]

    def on_policy_output(self, ac_data: ActionConnectorDataType):
        # Buffer latest output states for next input __call__.
        self._states[ac_data.env_id][ac_data.agent_id] = ac_data.output

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert (
            type(d) is dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        env_id = ac_data.env_id
        agent_id = ac_data.agent_id
        assert (
            env_id is not None and agent_id is not None
        ), f"StateBufferConnector requires env_id(f{env_id}) and agent_id(f{agent_id})"

        action, states, fetches = self._states[env_id][agent_id]

        if action is not None:
            d[SampleBatch.ACTIONS] = action  # Last action
        else:
            # Default zero action.
            d[SampleBatch.ACTIONS] = tree.map_structure(
                lambda s: np.zeros_like(s.sample(), s.dtype)
                if hasattr(s, "dtype")
                else np.zeros_like(s.sample()),
                self._action_space_struct,
            )

        if states is None:
            states = self._initial_states
        if self._enable_new_api_stack:
            if states:
                d[Columns.STATE_OUT] = states
        else:
            for i, v in enumerate(states):
                d["state_out_{}".format(i)] = v

        # Also add extra fetches if available.
        if fetches:
            d.update(fetches)

        return ac_data

    def to_state(self):
        # Note(jungong) : it is ok to use cloudpickle here for stats because:
        # 1. self._states may contain arbitary data objects, and will be hard
        #     to serialize otherwise.
        # 2. seriazlized states are only useful if a policy is stashed and
        #     restored during the rollout of a single episode. So it is ok to
        #     use cloudpickle for such non-persistent data bits.
        states = cloudpickle.dumps(self._states)
        return StateBufferConnector.__name__, states

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return StateBufferConnector(ctx, params)


register_connector(StateBufferConnector.__name__, StateBufferConnector)
