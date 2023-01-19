from typing import Any, Callable, Type

import numpy as np
import tree  # dm_tree

from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import (
    AgentConnectorDataType,
    AgentConnectorsOutput,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def register_lambda_agent_connector(
    name: str, fn: Callable[[Any], Any]
) -> Type[AgentConnector]:
    """A util to register any simple transforming function as an AgentConnector

    The only requirement is that fn should take a single data object and return
    a single data object.

    Args:
        name: Name of the resulting actor connector.
        fn: The function that transforms env / agent data.

    Returns:
        A new AgentConnector class that transforms data using fn.
    """

    class LambdaAgentConnector(AgentConnector):
        def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
            return AgentConnectorDataType(
                ac_data.env_id, ac_data.agent_id, fn(ac_data.data)
            )

        def to_state(self):
            return name, None

        @staticmethod
        def from_state(ctx: ConnectorContext, params: Any):
            return LambdaAgentConnector(ctx)

    LambdaAgentConnector.__name__ = name
    LambdaAgentConnector.__qualname__ = name

    register_connector(name, LambdaAgentConnector)

    return LambdaAgentConnector


@PublicAPI(stability="alpha")
def flatten_data(data: AgentConnectorsOutput):
    assert isinstance(
        data, AgentConnectorsOutput
    ), "Single agent data must be of type AgentConnectorsOutput"

    raw_dict = data.raw_dict
    sample_batch = data.sample_batch

    flattened = {}
    for k, v in sample_batch.items():
        if k in [SampleBatch.INFOS, SampleBatch.ACTIONS] or k.startswith("state_out_"):
            # Do not flatten infos, actions, and state_out_ columns.
            flattened[k] = v
            continue
        if v is None:
            # Keep the same column shape.
            flattened[k] = None
            continue
        flattened[k] = np.array(tree.flatten(v))
    flattened = SampleBatch(flattened, is_training=False)

    return AgentConnectorsOutput(raw_dict, flattened)


# Agent connector to build and return a flattened observation SampleBatch
# in addition to the original input dict.
FlattenDataAgentConnector = PublicAPI(stability="alpha")(
    register_lambda_agent_connector("FlattenDataAgentConnector", flatten_data)
)
