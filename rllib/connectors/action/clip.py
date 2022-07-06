from typing import Any, List

from ray.rllib.connectors.connector import (
    ActionConnector,
    ConnectorContext,
    register_connector,
)
from ray.rllib.utils.spaces.space_utils import clip_action, get_base_struct_from_space
from ray.rllib.utils.typing import ActionConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ClipActionsConnector(ActionConnector):
    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._action_space_struct = get_base_struct_from_space(ctx.action_space)

    def transform(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        assert isinstance(
            ac_data.output, tuple
        ), "Action connector requires PolicyOutputType data."

        actions, states, fetches = ac_data.output
        return ActionConnectorDataType(
            ac_data.env_id,
            ac_data.agent_id,
            (clip_action(actions, self._action_space_struct), states, fetches),
        )

    def to_config(self):
        return ClipActionsConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ClipActionsConnector(ctx)


register_connector(ClipActionsConnector.__name__, ClipActionsConnector)
