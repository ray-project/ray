from typing import Any

from ray.rllib.connectors.connector import (
    ActionConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.utils.spaces.space_utils import (
    get_base_struct_from_space,
    unsquash_action,
)
from ray.rllib.utils.typing import ActionConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class NormalizeActionsConnector(ActionConnector):
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
            ac_data.input_dict,
            (unsquash_action(actions, self._action_space_struct), states, fetches),
        )

    def to_state(self):
        return NormalizeActionsConnector.__name__, None

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return NormalizeActionsConnector(ctx)


register_connector(NormalizeActionsConnector.__name__, NormalizeActionsConnector)
