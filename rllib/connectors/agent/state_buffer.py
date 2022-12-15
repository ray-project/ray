from collections import defaultdict
from typing import Any

import numpy as np
import tree  # dm_tree

from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import ActionConnectorDataType, AgentConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class StateBufferConnector(AgentConnector):
    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._initial_states = ctx.initial_states
        self._action_space_struct = get_base_struct_from_space(ctx.action_space)
        self._states = defaultdict(lambda: defaultdict(lambda: (None, None, None)))

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
            type(d) == dict
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
        for i, v in enumerate(states):
            d["state_out_{}".format(i)] = v

        # Also add extra fetches if available.
        if fetches:
            d.update(fetches)

        return ac_data

    def to_state(self):
        return StateBufferConnector.__name__, None

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return StateBufferConnector(ctx)


register_connector(StateBufferConnector.__name__, StateBufferConnector)
