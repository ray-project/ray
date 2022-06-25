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
    AlgorithmConfigDict,
    TensorType,
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
        config: AlgorithmConfigDict = None,
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
    """Connector connecting user environments to RLlib policies.

    An agent connector transforms a single piece of data in AgentConnectorDataType
    format into a list of data in the same AgentConnectorDataTypes format.
    The API is designed so multi-agent observations can be broken and emitted as
    multiple single agent observations.

    AgentConnectorDataTypes can be used to specify arbitrary type of env data,

    Example:
    .. code-block:: python
        # A dict of multi-agent data from one env step() call.
        ac = AgentConnectorDataType(
            env_id="env_1",
            agent_id=None,
            data={
                "agent_1": np.array(...),
                "agent_2": np.array(...),
            }
        )

    Example:
    .. code-block:: python
        # Single agent data ready to be preprocessed.
        ac = AgentConnectorDataType(
            env_id="env_1",
            agent_id="agent_1",
            data=np.array(...)
        )

    We can adapt a simple stateless function into an agent connector by using
    register_lambda_agent_connector:
    .. code-block:: python
        TimesTwoAgentConnector = register_lambda_agent_connector(
            "TimesTwoAgentConnector", lambda data: data * 2
        )

    More complicated agent connectors can be implemented by extending this
    AgentConnector class:

    Example:
    .. code-block:: python
        class FrameSkippingAgentConnector(AgentConnector):
            def __init__(self, n):
                self._n = n
                self._frame_count = default_dict(str, default_dict(str, int))

            def reset(self, env_id: str):
                del self._frame_count[env_id]

            def __call__(
                self, ac_data: AgentConnectorDataType
            ) -> List[AgentConnectorDataType]:
                assert ac_data.env_id and ac_data.agent_id, (
                    "Frame skipping works per agent")

                count = self._frame_count[ac_data.env_id][ac_data.agent_id]
                self._frame_count[ac_data.env_id][ac_data.agent_id] = count + 1

                return [ac_data] if count % self._n == 0 else []

    As shown, an agent connector may choose to emit an empty list to stop input
    observations from being prosessed further.
    """

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
    """Action connector connects policy outputs including actions,
    to user environments.

    An action connector transforms a single piece of policy output in
    ActionConnectorDataType format, which is basically PolicyOutputType
    plus env and agent IDs.

    Any functions that operates directly on PolicyOutputType can be
    easily adpated into an ActionConnector by using register_lambda_action_connector.

    Example:
    .. code-block:: python
        ZeroActionConnector = register_lambda_action_connector(
            "ZeroActionsConnector",
            lambda actions, states, fetches: (
                np.zeros_like(actions), states, fetches
            )
        )

    More complicated action connectors can also be implemented by sub-classing
    this ActionConnector class.
    """

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
