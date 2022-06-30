from typing import Any, List

import tree  # pip install dm_tree

from ray.rllib.connectors.connector import (
    ActionConnector,
    ConnectorContext,
    register_connector,
)
from ray.rllib.utils.numpy import make_action_immutable
from ray.rllib.utils.typing import ActionConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
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
            (actions, states, fetches),
        )

    def to_config(self):
        return ImmutableActionsConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ImmutableActionsConnector(ctx)


register_connector(ImmutableActionsConnector.__name__, ImmutableActionsConnector)
