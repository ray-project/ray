"""This file defines base types and common structures for RLlib connectors.
"""

import abc
import gym
import logging
from typing import Any, Dict, List, Tuple

from ray.tune.registry import RLLIB_CONNECTOR, _global_registry
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    TensorType,
    TrainerConfigDict,
)

logger = logging.getLogger(__name__)


@DeveloperAPI
class ConnectorContext:
    """Data bits that may be needed for running connectors.

    Note(jungong) : we need to be really careful with the data fields here.
    E.g., everything needs to be serializable, in case we need to fetch them
    in a remote setting.
    """

    # TODO(jungong) : figure out how to fetch these in a remote setting.
    # Probably from a policy server when initializing a policy client.

    def __init__(
        self,
        config: TrainerConfigDict = None,
        model_initial_states: List[TensorType] = None,
        observation_space: gym.Space = None,
        action_space: gym.Space = None,
        view_requirements: Dict[str, ViewRequirement] = None,
    ):
        """Construct a ConnectorContext instance.

        Args:
            model_initial_states: States that are used for constructing
                the initial input dict for RNN models. [] if a model is not recurrent.
            action_space_struct: a policy's action space, in python
                data format. E.g., python dict instead of DictSpace, python tuple
                instead of TupleSpace.
        """
        self.config = config
        self.initial_states = model_initial_states or []
        self.observation_space = observation_space
        self.action_space = action_space
        self.view_requirements = view_requirements

    @staticmethod
    def from_policy(policy: Policy) -> "ConnectorContext":
        """Build ConnectorContext from a given policy.

        Args:
            policy: Policy

        Returns:
            A ConnectorContext instance.
        """
        return ConnectorContext(
            policy.config,
            policy.get_initial_state(),
            policy.observation_space,
            policy.action_space,
            policy.view_requirements,
        )


@DeveloperAPI
class Connector(abc.ABC):
    """Connector base class.

    A connector is a step of transformation, of either envrionment data before they
    get to a policy, or policy output before it is sent back to the environment.

    Connectors may be training-aware, for example, behave slightly differently
    during training and inference.

    All connectors are required to be serializable and implement to_config().
    """

    def __init__(self, ctx: ConnectorContext):
        # This gets flipped to False for inference.
        self.is_training = True

    def is_training(self, is_training: bool):
        self.is_training = is_training

    def to_config(self) -> Tuple[str, List[Any]]:
        """Serialize a connector into a JSON serializable Tuple.

        to_config is required, so that all Connectors are serializable.

        Returns:
            A tuple of connector's name and its serialized states.
        """
        # Must implement by each connector.
        return NotImplementedError

    @staticmethod
    def from_config(self, ctx: ConnectorContext, params: List[Any]) -> "Connector":
        """De-serialize a JSON params back into a Connector.

        from_config is required, so that all Connectors are serializable.

        Args:
            ctx: Context for constructing this connector.
            params: Serialized states of the connector to be recovered.

        Returns:
            De-serialized connector.
        """
        # Must implement by each connector.
        return NotImplementedError


@DeveloperAPI
class AgentConnector(Connector):
    """Connector connecting user environments to RLlib policies."""

    def reset(self, env_id: str):
        """Reset connector state for a specific environment.

        For example, at the end of an episode.

        Args:
            env_id: required. ID of a user environment. Required.
        """
        pass

    def on_policy_output(self, output: ActionConnectorDataType):
        """Callback on agent connector of policy output.

        This is useful for certain connectors, for example RNN state buffering,
        where the agent connect needs to be aware of the output of a policy
        forward pass.

        Args:
            ctx: Context for running this connector call.
            output: Env and agent IDs, plus data output from policy forward pass.
        """
        pass

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        """Transform incoming data from environment before they reach policy.

        Args:
            ctx: Context for running this connector call.
            data: Env and agent IDs, plus arbitrary data from an environment or
                upstream agent connectors.

        Returns:
            A list of transformed data in AgentConnectorDataType format.
            The return type is a list because an AgentConnector may choose to
            derive multiple outputs for a single input data, for example
            multi-agent obs -> multiple single agent obs.
            Agent connectors may also return an empty list for certain input,
            useful for connectors such as frame skipping.
        """
        raise NotImplementedError


@DeveloperAPI
class ActionConnector(Connector):
    def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        """Transform policy output before they are sent to a user environment.

        Args:
            ctx: Context for running this connector call.
            ac_data: Env and agent IDs, plus policy output.

        Returns:
            The processed action connector data.
        """
        raise NotImplementedError


@DeveloperAPI
class ConnectorPipeline:
    """Utility class for quick manipulation of a connector pipeline."""

    def remove(self, name: str):
        """Remove a connector by <name>

        Args:
            name: name of the connector to be removed.
        """
        idx = -1
        for idx, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                break
        if idx < 0:
            raise ValueError(f"Can not find connector {name}")
        del self.connectors[idx]

    def insert_before(self, name: str, connector: Connector):
        """Insert a new connector before connector <name>

        Args:
            name: name of the connector before which a new connector
                will get inserted.
            connector: a new connector to be inserted.
        """
        idx = -1
        for idx, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                break
        if idx < 0:
            raise ValueError(f"Can not find connector {name}")
        self.connectors.insert(idx, connector)

    def insert_after(self, name: str, connector: Connector):
        """Insert a new connector after connector <name>

        Args:
            name: name of the connector after which a new connector
                will get inserted.
            connector: a new connector to be inserted.
        """
        idx = -1
        for idx, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                break
        if idx < 0:
            raise ValueError(f"Can not find connector {name}")
        self.connectors.insert(idx + 1, connector)

    def prepend(self, connector: Connector):
        """Append a new connector at the beginning of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.insert(0, connector)

    def append(self, connector: Connector):
        """Append a new connector at the end of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.append(connector)


@DeveloperAPI
def register_connector(name: str, cls: Connector):
    """Register a connector for use with RLlib.

    Args:
        name: Name to register.
        cls: Callable that creates an env.
    """
    if not issubclass(cls, Connector):
        raise TypeError("Can only register Connector type.", cls)
    _global_registry.register(RLLIB_CONNECTOR, name, cls)


@DeveloperAPI
def get_connector(ctx: ConnectorContext, name: str, params: Tuple[Any]) -> Connector:
    """Get a connector by its name and serialized config.

    Args:
        name: name of the connector.
        params: serialized parameters of the connector.

    Returns:
        Constructed connector.
    """
    if not _global_registry.contains(RLLIB_CONNECTOR, name):
        raise NameError("connector not found.", name)
    cls = _global_registry.get(RLLIB_CONNECTOR, name)
    return cls.from_config(ctx, params)
