from typing import Any, Callable, Dict, Type

from ray.rllib.connectors.connector import (
    ActionConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    PolicyOutputType,
    StateBatches,
    TensorStructType,
)
from ray.util.annotations import PublicAPI


def connector_from_lambda(
    name: str,
    fn: Callable[
        [],
    ],
):
    """A util to register any function transforming PolicyOutputType as an ActionConnector.

    The only requirement is that fn should take actions, states, and fetches as input,
    and return transformed actions, states, and fetches.

    Args:
        name: Name of the resulting actor connector.
        fn: The function that transforms PolicyOutputType.

    Returns:
        A new ActionConnector class that transforms PolicyOutputType using fn.
    """

    class LambdaConnector(Connector):
        def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
            actions, states, fetches = ac_data.output
            return ActionConnectorDataType(
                ac_data.env_id,
                ac_data.agent_id,
                ac_data.input_dict,
                fn(actions, states, fetches),
            )

        @override(ActionConnector)
        def serialize(self):
            return name, None

        @staticmethod
        def from_state(ctx: ConnectorContext, params: Any):
            return LambdaActionConnector(ctx)

    LambdaActionConnector.__name__ = name
    LambdaActionConnector.__qualname__ = name

    # register_connector(name, LambdaActionConnector)

    return LambdaConnector
