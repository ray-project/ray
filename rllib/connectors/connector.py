"""This file defines base types and common structures for RLlib connectors.
"""

import abc
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import gymnasium as gym

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.connectors.input_output_types import INPUT_OUTPUT_TYPE
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.deprecation import Deprecated
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
    """Information needed by pieces of connector pipeline to communicate with each other.

    Note: We need to be really careful with the data fields here.
    E.g., everything needs to be serializable, in case we need to fetch them
    in a remote setting.
    """

    # TODO(jungong) : figure out how to fetch these in a remote setting.
    #  Probably from a policy server when initializing a policy client.

    def __init__(
        self,
        config: AlgorithmConfigDict = None,
        initial_states: List[TensorType] = None,  # TODO (sven): deprecate
        observation_space: gym.Space = None,
        action_space: gym.Space = None,
        view_requirements: Dict[str, ViewRequirement] = None,  # TODO (sven): deprecate
        is_policy_recurrent: bool = False,  # TODO (sven): deprecate
    ):
        """Initializes a ConnectorContext instance."""
        self.config = config or {}
        self.initial_states = initial_states or []
        self.observation_space = observation_space
        self.action_space = action_space
        self.view_requirements = view_requirements
        self.is_policy_recurrent = is_policy_recurrent

        self._state = {}

    # TODO (sven): Deprecate along with Policy deprecation.
    @staticmethod
    def from_policy(policy: "Policy") -> "ConnectorContext":
        """Build ConnectorContext from a given policy.

        Args:
            policy: Policy

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
        )


@PublicAPI(stability="alpha")
class ConnectorContextV2:
    """Information needed by pieces of connector pipeline to communicate with each other.

    ConnectorContextV2 will be passed to each connector (pipeline) call.
    Also might contain references to the RLModule used, the Env, as well as whether
    `explore` is True or False (whether forward_exploration or forward_inference was
    used).

    TODO: Describe use cases, e.g.
     - state out need to be fed back as state ins.
     Unless we would like to temporarily store the states in the episode.
     - agent_to_policy_mappings need to be stored as they might be stochastic. Then the
     to_env pipeline can properly map back from module (formerly known as policy) IDs
     to agent IDs.
    """

    def __init__(
        self,
        rl_module: Optional[RLModule] = None,
        env: Optional[gym.Env] = None,
        explore: Optional[bool] = None,
        *,
        data: Optional[Any] = None,
    ):
        """Initializes a ConnectorContextV2 instance."""

        self.rl_module = rl_module
        self.env = env
        self.explore = explore

        self._data = data or {}

    def add_data(self, key, value):
        assert key not in self._data
        self._data[key] = value

    def get_data(self, key):
        assert key in self._data
        return self._data[key]

    def override_data(self, key, value):
        assert key in self._data
        self._data[key] = value

    def del_data(self, key):
        assert key in self._data
        del self._data[key]


@PublicAPI(stability="alpha")
class Connector(abc.ABC):
    """Connector base class.

    A connector performs a transformation step, either on envrionment data before it
    gets to the RLModule, or on RLModule output before it is sent back to the
    environment.

    Connectors may be training-aware, for example, behave slightly differently
    during training and inference.

    All connectors are required to be serializable and implement the `serialize()` method.
    """

    def __init__(
        self,
        ctx: Optional[ConnectorContext] = None,  # TODO (sven): deprecate
        *,
        config: Optional[AlgorithmConfig] = None,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        input_type: INPUT_OUTPUT_TYPE = INPUT_OUTPUT_TYPE.DATA,
        output_type: INPUT_OUTPUT_TYPE = INPUT_OUTPUT_TYPE.DATA,
        
    ):
        self.config = config
        self.observation_space = observation_space
        self.action_space = action_space
        self.input_type = input_type
        self.output_type = output_type

        self._is_training = True  # TODO (sven): deprecate

    # TODO (sven): deprecate
    def in_training(self):
        self._is_training = True

    # TODO (sven): deprecate
    def in_eval(self):
        self._is_training = False

    @abc.abstractmethod
    def __call__(
        self,
        *,
        input_: Any,
        episodes: List[_Episode],  # TODO: generalize to be also MultiAgentEpisode
        ctx: ConnectorContextV2,
    ) -> Any:
        """Method for transforming input data into output data.

        Args:
            input_: The input data to be transformed by this connector. Transformations
                might either be done in-place or a new structure may be returned that
                matches `self.output_type`.
            episodes: The list of _Episode objects, each corresponding to one slot in the
                vector env. Note that Episodes should always be considered read-only
                and not be altered.
            ctx: The ConnectorContext that might be used to pass along other important
                information in between connector pieces (even across pipelines).

        Returns:
            The transformed connector output.
        """

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__

    def serialize(self) -> Tuple[str, Any]:
        """Serialize a connector into a JSON serializable Tuple.

        `serialize()` is required, so that all Connectors are serializable.

        Returns:
            A tuple of connector's name and its serialized states.
            String should match the name used to register the connector,
            while state can be any single data structure that contains the
            serialized state of the connector. If a connector is stateless,
            state can simply be None.
        """
        # Must implement by each connector.
        raise NotImplementedError

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any) -> "Connector":
        """De-serialize a JSON params back into a Connector.

        `from_state()` is required, so that all Connectors are serializable.

        Args:
            ctx: Context for constructing this connector.
            params: Serialized states of the connector to be recovered.

        Returns:
            De-serialized connector.
        """
        # Must implement by each connector.
        raise NotImplementedError

    @Deprecated(new="Connector.serialize()", error=False)
    def to_state(self):
        return self.serialize()


class FromEnvConnector(Connector):
    pass


class FromModuleConnector(Connector):
    pass


class TrainingConnector(Connector):
    pass


# TODO (sven): deprecate
@PublicAPI(stability="alpha")
class AgentConnector(Connector):
    """Connector connecting user environments to RLlib policies.

    An agent connector transforms a list of agent data in AgentConnectorDataType
    format into a new list in the same AgentConnectorDataTypes format.
    The input API is designed so agent connectors can have access to all the
    agents assigned to a particular policy.

    AgentConnectorDataTypes can be used to specify arbitrary types of env data,

    Example:

        Represent a list of agent data from one env step() call.

        .. testcode::

            import numpy as np
            ac = AgentConnectorDataType(
                env_id="env_1",
                agent_id=None,
                data={
                    "agent_1": np.array([1, 2, 3]),
                    "agent_2": np.array([4, 5, 6]),
                }
            )

        Or a single agent data ready to be preprocessed.

        .. testcode::

            ac = AgentConnectorDataType(
                env_id="env_1",
                agent_id="agent_1",
                data=np.array([1, 2, 3]),
            )

        We can also adapt a simple stateless function into an agent connector by
        using register_lambda_agent_connector:

        .. testcode::

            import numpy as np
            from ray.rllib.connectors.agent.lambdas import (
                register_lambda_agent_connector
            )
            TimesTwoAgentConnector = register_lambda_agent_connector(
                "TimesTwoAgentConnector", lambda data: data * 2
            )

            # More complicated agent connectors can be implemented by extending this
            # AgentConnector class:

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
                        assert d.env_id and d.agent_id, "Skipping works per agent!"

                        count = self._frame_count[ac_data.env_id][ac_data.agent_id]
                        self._frame_count[ac_data.env_id][ac_data.agent_id] = (
                            count + 1
                        )

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
            env_id: The ID of the environment.
        """
        pass

    # TODO (sven): Deprecate in favor of Episodes (which contain all the needed data).
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
            acd_list: List of `AgentConnectorDataType`, each containing env-, agent ID,
                and arbitrary data items from an environment or upstream agent connectors.

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
            ac_data: Env and agent IDs, plus arbitrary data item from a single agent
                of an environment.

        Returns:
            A transformed piece of agent connector data.
        """
        raise NotImplementedError


# TODO (sven): deprecate
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

    .. testcode::

        from ray.rllib.connectors.action.lambdas import (
            register_lambda_action_connector
        )
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
