from collections import defaultdict
import numpy as np
import tree  # dm_tree
from typing import Any, List

from ray.rllib.connectors.connector import (
    ConnectorContext,
    AgentConnector,
    register_connector,
)
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import (
    AgentConnectorDataType,
    PolicyOutputType,
)


@DeveloperAPI
class _AgentState(object):
    def __init__(self):
        self.t = 0
        self.action = None
        self.states = None


@DeveloperAPI
class StateBufferConnector(AgentConnector):
    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._initial_states = ctx.initial_states
        self._action_space_struct = get_base_struct_from_space(ctx.action_space)
        self._states = defaultdict(lambda: defaultdict(_AgentState))

    def reset(self, env_id: str):
        del self._states[env_id]

    def on_policy_output(self, env_id: str, agent_id: str, output: PolicyOutputType):
        # Buffer latest output states for next input __call__.
        action, states, _ = output
        agent_state = self._states[env_id][agent_id]
        agent_state.action = convert_to_numpy(action)
        agent_state.states = convert_to_numpy(states)

    def __call__(
        self, ctx: ConnectorContext, ac_data: AgentConnectorDataType
    ) -> List[AgentConnectorDataType]:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        env_id = ac_data.env_id
        agent_id = ac_data.agent_id
        assert env_id and agent_id, "StateBufferConnector requires env_id and agent_id"

        agent_state = self._states[env_id][agent_id]

        d.update(
            {
                SampleBatch.T: agent_state.t,
                SampleBatch.ENV_ID: env_id,
            }
        )

        if agent_state.states is not None:
            states = agent_state.states
        else:
            states = self._initial_states
        for i, v in enumerate(states):
            d["state_out_{}".format(i)] = v

        if agent_state.action is not None:
            d[SampleBatch.ACTIONS] = agent_state.action  # Last action
        else:
            # Default zero action.
            d[SampleBatch.ACTIONS] = tree.map_structure(
                lambda s: np.zeros_like(s.sample(), s.dtype)
                if hasattr(s, "dtype")
                else np.zeros_like(s.sample()),
                self._action_space_struct,
            )

        agent_state.t += 1

        return [ac_data]

    def to_config(self):
        return StateBufferConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return StateBufferConnector(ctx)


register_connector(StateBufferConnector.__name__, StateBufferConnector)
