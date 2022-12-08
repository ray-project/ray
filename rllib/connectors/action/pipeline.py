import logging
from typing import Any, List

from ray.rllib.connectors.connector import (
    ActionConnector,
    Connector,
    ConnectorContext,
    ConnectorPipeline,
)
from ray.rllib.connectors.registry import get_connector, register_connector
from ray.rllib.utils.typing import ActionConnectorDataType
from ray.util.annotations import PublicAPI


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class ActionConnectorPipeline(ConnectorPipeline, ActionConnector):
    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        super().__init__(ctx, connectors)

    def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        for c in self.connectors:
            ac_data = c(ac_data)
        return ac_data

    def to_state(self):
        children = []
        for c in self.connectors:
            state = c.to_state()
            assert isinstance(state, tuple) and len(state) == 2, (
                "Serialized connector state must be in the format of "
                f"Tuple[name: str, params: Any]. Instead we got {state}"
                f"for connector {c.__name__}."
            )
            children.append(state)
        return ActionConnectorPipeline.__name__, children

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        assert (
            type(params) == list
        ), "ActionConnectorPipeline takes a list of connector params."
        connectors = []
        for state in params:
            try:
                name, subparams = state
                connectors.append(get_connector(name, ctx, subparams))
            except Exception as e:
                logger.error(f"Failed to de-serialize connector state: {state}")
                raise e
        return ActionConnectorPipeline(ctx, connectors)


register_connector(ActionConnectorPipeline.__name__, ActionConnectorPipeline)
