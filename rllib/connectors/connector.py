"""This file defines base types and common structures for RLlib connectors.
"""

import abc
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union

import gym

from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    AlgorithmConfigDict,
    TensorType,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.policy.policy import Policy

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class ConnectorContext:
    """Data bits that may be needed for running connectors.

    Note(jungong) : we need to be really careful with the data fields here.
    E.g., everything needs to be serializable, in case we need to fetch them
    in a remote setting.
    """

    # TODO(jungong) : figure out how to fetch these in a remote setting.
    #  Probably from a policy server when initializing a policy client.

    def __init__(
        self,
        config: AlgorithmConfigDict = None,
        initial_states: List[TensorType] = None,
        observation_space: gym.Space = None,
        action_space: gym.Space = None,
        view_requirements: Dict[str, ViewRequirement] = None,
        is_policy_recurrent: bool = False,
        is_atari: bool = False,
        preprocessing_enabled: bool = True,
    ):
        """Construct a ConnectorContext instance.

        Args:
            config: Algorithm config dict.
            initial_states: States that are used for constructing
                the initial input dict for RNN models. [] if a model is not recurrent.
            observation_space: a policy's observation space.
            action_space_struct: a policy's action space, in python
                data format. E.g., python dict instead of DictSpace, python tuple
                instead of TupleSpace.
            view_requirements: a policy's view requirements.
            is_policy_recurrent: whether the policy is recurrent.
            is_atari: whether the policy is for an Atari env.
            preprocessing_enabled: whether automatic preprocessing is enabled.
        """
        self.config = config or {}
        self.initial_states = initial_states or []
        self.observation_space = observation_space
        self.action_space = action_space
        self.view_requirements = view_requirements
        self.is_policy_recurrent = is_policy_recurrent
        self.is_atari = is_atari
        self.preprocessing_enabled = preprocessing_enabled

    @staticmethod
    def from_policy(
        policy: "Policy",
        *,
        is_atari: bool = False,
        preprocessing_enabled: bool = True,
    ) -> "ConnectorContext":
        """Build ConnectorContext from a given policy.

        Args:
            policy: Policy
            is_atari: Whether the policy is for an Atari env.
                Note that this config is not available from the policy today.
            preprocessing_enabled: Whether automatic preprocessing is enabled
                for this policy. Note that this config is also not available from
                the policy itself, since preprocessing happens before sample batches
                arrive at the policy.

        Returns:
            A ConnectorContext instance.
        """
        return ConnectorContext(
            config=policy.config,
            initial_states=policy.get_initial_state(),
            observation_space=policy.observation_space,
            action_space=policy.action_space,
            view_requirements=policy.view_requirements,
            is_policy_recurrent=policy.is_recurrent(),
            is_atari=is_atari,
            preprocessing_enabled=preprocessing_enabled,
        )


@PublicAPI(stability="alpha")
class Connector(abc.ABC):
    """Connector base class.

    A connector is a step of transformation, of either envrionment data before they
    get to a policy, or policy output before it is sent back to the environment.

    Connectors may be training-aware, for example, behave slightly differently
    during training and inference.

    All connectors are required to be serializable and implement to_state().
    """

    def __init__(self, ctx: ConnectorContext):
        # Default is training mode.
        self._is_training = True

    def in_training(self):
        self._is_training = True

    def in_eval(self):
        self._is_training = False

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__

    def to_state(self) -> Tuple[str, Any]:
        """Serialize a connector into a JSON serializable Tuple.

        to_state is required, so that all Connectors are serializable.

        Returns:
            A tuple of connector's name and its serialized states.
            String should match the name used to register the connector,
            while state can be any single data structure that contains the
            serialized state of the connector. If a connector is stateless,
            state can simply be None.
        """
        # Must implement by each connector.
        return NotImplementedError

    @staticmethod
    def from_state(self, ctx: ConnectorContext, params: Any) -> "Connector":
        """De-serialize a JSON params back into a Connector.

        from_state is required, so that all Connectors are serializable.

        Args:
            ctx: Context for constructing this connector.
            params: Serialized states of the connector to be recovered.

        Returns:
            De-serialized connector.
        """
        # Must implement by each connector.
        return NotImplementedError


@PublicAPI(stability="alpha")
class AgentConnector(Connector):
    """Connector connecting user environments to RLlib policies.

    An agent connector transforms a list of agent data in AgentConnectorDataType
    format into a new list in the same AgentConnectorDataTypes format.
    The input API is designed so agent connectors can have access to all the
    agents assigned to a particular policy.

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
                self, ac_data: List[AgentConnectorDataType]
            ) -> List[AgentConnectorDataType]:
                ret = []
                for d in ac_data:
                    assert d.env_id and d.agent_id, "Frame skipping works per agent"

                    count = self._frame_count[ac_data.env_id][ac_data.agent_id]
                    self._frame_count[ac_data.env_id][ac_data.agent_id] = count + 1

                    if count % self._n == 0:
                        ret.append(d)
                return ret

    As shown, an agent connector may choose to emit an empty list to stop input
    observations from being further prosessed.
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

    def __call__(
        self, acd_list: List[AgentConnectorDataType]
    ) -> List[AgentConnectorDataType]:
        """Transform a list of data items from env before they reach policy.

        Args:
            ac_data: List of env and agent IDs, plus arbitrary data items from
                an environment or upstream agent connectors.

        Returns:
            A list of transformed data items in AgentConnectorDataType format.
            The shape of a returned list does not have to match that of the input list.
            An AgentConnector may choose to derive multiple outputs for a single piece
            of input data, for example multi-agent obs -> multiple single agent obs.
            Agent connectors may also choose to skip emitting certain inputs,
            useful for connectors such as frame skipping.
        """
        assert isinstance(
            acd_list, (list, tuple)
        ), "Input to agent connectors are list of AgentConnectorDataType."
        # Default implementation. Simply call transform on each agent connector data.
        return [self.transform(d) for d in acd_list]

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        """Transform a single agent connector data item.

        Args:
            data: Env and agent IDs, plus arbitrary data item from a single agent
            of an environment.

        Returns:
            A transformed piece of agent connector data.
        """
        raise NotImplementedError


@PublicAPI(stability="alpha")
class ActionConnector(Connector):
    """Action connector connects policy outputs including actions,
    to user environments.

    An action connector transforms a single piece of policy output in
    ActionConnectorDataType format, which is basically PolicyOutputType plus env and
    agent IDs.

    Any functions that operate directly on PolicyOutputType can be easily adapted
    into an ActionConnector by using register_lambda_action_connector.

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
            ac_data: Env and agent IDs, plus policy output.

        Returns:
            The processed action connector data.
        """
        return self.transform(ac_data)

    def transform(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        """Implementation of the actual transform.

        Users should override transform instead of __call__ directly.

        Args:
            ac_data: Env and agent IDs, plus policy output.

        Returns:
            The processed action connector data.
        """
        raise NotImplementedError


@PublicAPI(stability="alpha")
class ConnectorPipeline(abc.ABC):
    """Utility class for quick manipulation of a connector pipeline."""

    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        self.connectors = connectors

    def in_training(self):
        for c in self.connectors:
            c.in_training()

    def in_eval(self):
        for c in self.connectors:
            c.in_eval()

    def remove(self, name: str):
        """Remove a connector by <name>

        Args:
            name: name of the connector to be removed.
        """
        idx = -1
        for i, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                idx = i
                break
        if idx >= 0:
            del self.connectors[idx]
            logger.info(f"Removed connector {name} from {self.__class__.__name__}.")
        else:
            logger.warning(f"Trying to remove a non-existent connector {name}.")

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

        logger.info(
            f"Inserted {connector.__class__.__name__} before {name} "
            f"to {self.__class__.__name__}."
        )

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

        logger.info(
            f"Inserted {connector.__class__.__name__} after {name} "
            f"to {self.__class__.__name__}."
        )

    def prepend(self, connector: Connector):
        """Append a new connector at the beginning of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.insert(0, connector)

        logger.info(
            f"Added {connector.__class__.__name__} to the beginning of "
            f"{self.__class__.__name__}."
        )

    def append(self, connector: Connector):
        """Append a new connector at the end of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.append(connector)

        logger.info(
            f"Added {connector.__class__.__name__} to the end of "
            f"{self.__class__.__name__}."
        )

    def __str__(self, indentation: int = 0):
        return "\n".join(
            [" " * indentation + self.__class__.__name__]
            + [c.__str__(indentation + 4) for c in self.connectors]
        )

    def __getitem__(self, key: Union[str, int, type]):
        """Returns a list of connectors that fit 'key'.

        If key is a number n, we return a list with the nth element of this pipeline.
        If key is a Connector class or a string matching the class name of a
        Connector class, we return a list of all connectors in this pipeline matching
        the specified class.

        Args:
            key: The key to index by

        Returns: The Connector at index `key`.
        """
        # In case key is a class
        if not isinstance(key, str):
            if isinstance(key, slice):
                raise NotImplementedError(
                    "Slicing of ConnectorPipeline is currently not supported."
                )
            elif isinstance(key, int):
                return [self.connectors[key]]
            elif isinstance(key, type):
                results = []
                for c in self.connectors:
                    if issubclass(c.__class__, key):
                        results.append(c)
                return results
            else:
                raise NotImplementedError(
                    "Indexing by {} is currently not supported.".format(type(key))
                )

        results = []
        for c in self.connectors:
            if c.__class__.__name__ == key:
                results.append(c)

        return results
