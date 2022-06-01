from typing import Any, Callable, Dict, List, Type

from ray.rllib.connectors.connector import (
    Connector,
    ConnectorContext,
    ConnectorPipeline,
    ActionConnector,
    register_connector,
    get_connector,
)
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import (
    clip_action,
    get_base_struct_from_space,
    unsquash_action,
    unbatch,
)
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    PolicyOutputType,
    StateBatch,
    TensorStructType,
    TrainerConfigDict,
)


@DeveloperAPI
def register_lambda_action_connector(
    name: str,
    fn: Callable[[TensorStructType, StateBatch, Dict], PolicyOutputType]
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
        def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
            assert isinstance(
                ac_data.output, tuple
            ), "Action connector requires PolicyOutputType data."

            actions, states, fetches = ac_data.output
            return ActionConnectorDataType(
                ac_data.env_id,
                ac_data.agent_id,
                fn(actions, states, fetches),
            )

        def to_config(self):
            return name, None

        @staticmethod
        def from_config(ctx: ConnectorContext, params: List[Any]):
            return LambdaActionConnector(ctx)

    LambdaActionConnector.__name__ = name
    LambdaActionConnector.__qualname__ = name

    register_connector(name, LambdaActionConnector)

    return LambdaActionConnector


# Convert actions and states into numpy arrays if necessary.
ConvertToNumpyConnector = register_lambda_action_connector(
    "ConvertToNumpyConnector",
    lambda actions, states, fetches: (
        convert_to_numpy(actions), convert_to_numpy(states), fetches
    )
)


# Split action-component batches into single action rows.
UnbatchActionsConnector = register_lambda_action_connector(
    "UnbatchActionsConnector",
    lambda actions, states, fetches: (
        unbatch(actions), states, fetches
    )
)


@DeveloperAPI
class NormalizeActionsConnector(ActionConnector):
    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._action_space_struct = get_base_struct_from_space(ctx.action_space)

    def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        assert isinstance(
            ac_data.output, tuple
        ), "Action connector requires PolicyOutputType data."

        actions, states, fetches = ac_data.output
        return ActionConnectorDataType(
            ac_data.env_id,
            ac_data.agent_id,
            (unsquash_action(actions, self._action_space_struct), states, fetches),
        )

    def to_config(self):
        return NormalizeActionsConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return NormalizeActionsConnector(ctx)


register_connector(NormalizeActionsConnector.__name__, NormalizeActionsConnector)


@DeveloperAPI
class ClipActionsConnector(ActionConnector):
    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._action_space_struct = get_base_struct_from_space(ctx.action_space)

    def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
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


@DeveloperAPI
class ActionConnectorPipeline(ActionConnector, ConnectorPipeline):
    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        super().__init__(ctx)
        self.connectors = connectors

    def is_training(self, is_training: bool):
        self.is_training = is_training
        for c in self.connectors:
            c.is_training(is_training)

    def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        for c in self.connectors:
            ac_data = c(ac_data)
        return ac_data

    def to_config(self):
        return ActionConnectorPipeline.__name__, [
            c.to_config() for c in self.connectors
        ]

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        assert (
            type(params) == list
        ), "ActionConnectorPipeline takes a list of connector params."
        connectors = [get_connector(ctx, name, subparams) for name, subparams in params]
        return ActionConnectorPipeline(ctx, connectors)


register_connector(ActionConnectorPipeline.__name__, ActionConnectorPipeline)


@DeveloperAPI
def get_action_connectors_from_trainer_config(
    config: TrainerConfigDict, action_space: gym.Space
) -> ActionConnectorPipeline:
    connectors = [ConvertToNumpyConnector()]
    return ActionConnectorPipeline(connectors)
