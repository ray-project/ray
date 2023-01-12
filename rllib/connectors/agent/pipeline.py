import logging
from typing import Any, List
from collections import defaultdict

from ray.rllib.connectors.connector import (
    AgentConnector,
    Connector,
    ConnectorContext,
    ConnectorPipeline,
)
from ray.rllib.connectors.registry import get_connector, register_connector
from ray.rllib.utils.typing import ActionConnectorDataType, AgentConnectorDataType
from ray.util.annotations import PublicAPI
from ray.util.timer import _Timer


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class AgentConnectorPipeline(ConnectorPipeline, AgentConnector):
    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        super().__init__(ctx, connectors)
        self.timers = defaultdict(_Timer)

    def reset(self, env_id: str):
        for c in self.connectors:
            c.reset(env_id)
        self.timers.clear()

    def on_policy_output(self, output: ActionConnectorDataType):
        for c in self.connectors:
            c.on_policy_output(output)

    def __call__(
        self, acd_list: List[AgentConnectorDataType]
    ) -> List[AgentConnectorDataType]:
        ret = acd_list
        for c in self.connectors:
            timer = self.timers[str(c)]
            with timer:
                ret = c(ret)
            timer.push_units_processed(1)
        return ret

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
        return AgentConnectorPipeline.__name__, children

    @staticmethod
    def from_state(ctx: ConnectorContext, params: List[Any]):
        assert (
            type(params) == list
        ), "AgentConnectorPipeline takes a list of connector params."
        connectors = []
        for state in params:
            try:
                name, subparams = state
                connectors.append(get_connector(name, ctx, subparams))
            except Exception as e:
                logger.error(f"Failed to de-serialize connector state: {state}")
                raise e
        return AgentConnectorPipeline(ctx, connectors)


register_connector(AgentConnectorPipeline.__name__, AgentConnectorPipeline)
