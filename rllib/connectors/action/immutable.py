from typing import Any

import tree  # pip install dm_tree

from ray.rllib.connectors.connector import (
    ActionConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.numpy import make_action_immutable
from ray.rllib.utils.typing import ActionConnectorDataType


@OldAPIStack
class ImmutableActionsConnector(ActionConnector):
    def transform(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        assert isinstance(
            ac_data.output, tuple
        ), "Action connector requires PolicyOutputType data."

        actions, states, fetches = ac_data.output
        tree.traverse(make_action_immutable, actions, top_down=False)

        return ActionConnectorDataType(
            ac_data.env_id,
            ac_data.agent_id,
            ac_data.input_dict,
            (actions, states, fetches),
        )

    def to_state(self):
        return ImmutableActionsConnector.__name__, None

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return ImmutableActionsConnector(ctx)


register_connector(ImmutableActionsConnector.__name__, ImmutableActionsConnector)
