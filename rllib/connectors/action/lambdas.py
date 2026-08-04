from typing import Any, Callable, Dict, Type

from ray.rllib.connectors.connector import (
    ActionConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    PolicyOutputType,
    StateBatches,
    TensorStructType,
)


@OldAPIStack
def register_lambda_action_connector(
    name: str, fn: Callable[[TensorStructType, StateBatches, Dict], PolicyOutputType]
) -> Type[ActionConnector]:
    """A util to register any function transforming PolicyOutputType as an ActionConnector.

    The only requirement is that fn should take actions, states, and fetches as input,
    and return transformed actions, states, and fetches.

    Args:
        name: Name of the resulting actor connector.
        fn: The function that transforms PolicyOutputType.

    Returns:
        A new ActionConnector class that transforms PolicyOutputType using fn.
    """

    class LambdaActionConnector(ActionConnector):
        def transform(
            self, ac_data: ActionConnectorDataType
        ) -> ActionConnectorDataType:
            assert isinstance(
                ac_data.output, tuple
            ), "Action connector requires PolicyOutputType data."

            actions, states, fetches = ac_data.output
            return ActionConnectorDataType(
                ac_data.env_id,
                ac_data.agent_id,
                ac_data.input_dict,
                fn(actions, states, fetches),
            )

        def to_state(self):
            return name, None

        @staticmethod
        def from_state(ctx: ConnectorContext, params: Any):
            return LambdaActionConnector(ctx)

    LambdaActionConnector.__name__ = name
    LambdaActionConnector.__qualname__ = name

    register_connector(name, LambdaActionConnector)

    return LambdaActionConnector


# Convert actions and states into numpy arrays if necessary.
ConvertToNumpyConnector = OldAPIStack(
    register_lambda_action_connector(
        "ConvertToNumpyConnector",
        lambda actions, states, fetches: (
            convert_to_numpy(actions),
            convert_to_numpy(states),
            fetches,
        ),
    ),
)
