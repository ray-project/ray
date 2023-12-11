from collections import defaultdict
import logging
from typing import Any, List, Optional, Union

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.env_to_module.default_env_to_module import DefaultEnvToModule
from ray.rllib.connectors.module_to_env.default_module_to_env import DefaultModuleToEnv
from ray.rllib.connectors.learner.default_learner_connector import (
    DefaultLearnerConnector
)
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI
from ray.util.timer import _Timer

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class ConnectorPipelineV2(ConnectorV2):
    """Utility class for quick manipulation of a connector pipeline."""

    def __init__(
        self,
        *,
        connectors: Optional[List[ConnectorV2]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.connectors = connectors or []
        self._fix_input_output_types()

        self.timers = defaultdict(_Timer)

    def remove(self, name: str):
        """Remove a connector piece by <name>.

        Args:
            name: The name of the connector piece to be removed from the pipeline.
        """
        idx = -1
        for i, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                idx = i
                break
        if idx >= 0:
            del self.connectors[idx]
            self._fix_input_output_types()
            logger.info(f"Removed connector {name} from {self.__class__.__name__}.")
        else:
            logger.warning(f"Trying to remove a non-existent connector {name}.")

    def insert_before(self, name: str, connector: ConnectorV2):
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
        self._fix_input_output_types()

        logger.info(
            f"Inserted {connector.__class__.__name__} before {name} "
            f"to {self.__class__.__name__}."
        )

    def insert_after(self, name: str, connector: ConnectorV2):
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
        self._fix_input_output_types()

        logger.info(
            f"Inserted {connector.__class__.__name__} after {name} "
            f"to {self.__class__.__name__}."
        )

    def prepend(self, connector: ConnectorV2):
        """Append a new connector at the beginning of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.insert(0, connector)
        self._fix_input_output_types()

        logger.info(
            f"Added {connector.__class__.__name__} to the beginning of "
            f"{self.__class__.__name__}."
        )

    def append(self, connector: ConnectorV2):
        """Append a new connector at the end of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.append(connector)
        self._fix_input_output_types()

        logger.info(
            f"Added {connector.__class__.__name__} to the end of "
            f"{self.__class__.__name__}."
        )

    def __call__(
        self,
        rl_module: RLModule,
        input_: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        ret = input_
        for connector in self.connectors:
            timer = self.timers[str(connector)]
            with timer:
                ret = connector(
                    rl_module=rl_module,
                    input_=ret,
                    episodes=episodes,
                    explore=explore,
                    persistent_data=persistent_data,
                    **kwargs,
                )
        return ret

    # @override(ConnectorV2)
    # def serialize(self):
    #    children = []
    #    for c in self.connectors:
    #        state = c.serialize()
    #        assert isinstance(state, tuple) and len(state) == 2, (
    #            "Serialized connector state must be in the format of "
    #            f"Tuple[name: str, params: Any]. Instead we got {state}"
    #            f"for connector {c.__name__}."
    #        )
    #        children.append(state)
    #    return ConnectorPipelineV2.__name__, children
    #
    # @override(ConnectorV2)
    # @staticmethod
    # def from_state(ctx: ConnectorContextV2, params: List[Any]):
    #    assert (
    #        type(params) == list
    #    ), "AgentConnectorPipeline takes a list of connector params."
    #    connectors = []
    #    for state in params:
    #        try:
    #            name, subparams = state
    #            connectors.append(get_connector(name, ctx, subparams))
    #        except Exception as e:
    #            logger.error(f"Failed to de-serialize connector state: {state}")
    #            raise e
    #    return ConnectorPipelineV2(ctx, connectors)

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

    def _fix_input_output_types(self):
        if len(self.connectors) > 0:
            self.input_type = self.connectors[0].input_type
            self.output_type = self.connectors[-1].output_type
            #self.observation_space = self.connectors[-1].observation_space
            #self.action_space = self.connectors[-1].action_space
        else:
            self.input_type = None
            self.output_type = None


class EnvToModulePipeline(ConnectorPipelineV2):
    def __init__(
        self,
        *,
        connectors: Optional[List[ConnectorV2]] = None,
        input_observation_space: Optional[gym.Space],
        input_action_space: Optional[gym.Space],
        env: Optional[gym.Env] = None,
        rl_module: Optional["RLModule"] = None,
        **kwargs,
    ):
        super().__init__(
            connectors=connectors,
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            env=env,
            rl_module=rl_module,
            **kwargs,
        )
        # Add the default final connector piece for env-to-module pipelines:
        # Extracting last obs from episodes and add them to input, iff this has not
        # happened in any connector piece in this pipeline before.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultEnvToModule
        ):
            self.append(DefaultEnvToModule(
                input_observation_space=self.observation_space,
                input_action_space=self.action_space,
                env=env,
            ))

    def __call__(
        self,
        *,
        rl_module: RLModule,
        input_: Optional[Any] = None,
        episodes: List[EpisodeType],
        explore: bool,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ):
        # Make sure user does not necessarily send initial input into this pipeline.
        # Might just be empty and to be populated from `episodes`.
        return super().__call__(
            rl_module=rl_module,
            input_=input_ if input_ is not None else {},
            episodes=episodes,
            explore=explore,
            persistent_data=persistent_data,
            **kwargs,
        )


class ModuleToEnvPipeline(ConnectorPipelineV2):
    def __init__(
        self,
        *,
        connectors: Optional[List[ConnectorV2]] = None,
        input_observation_space: Optional[gym.Space],
        input_action_space: Optional[gym.Space],
        env: Optional[gym.Env] = None,
        rl_module: Optional["RLModule"] = None,
        **kwargs,
    ):
        super().__init__(
            connectors=connectors,
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            env=env,
            rl_module=rl_module,
            **kwargs,
        )

        # Add the default final connector piece for env-to-module pipelines:
        # Sampling actions from action_dist_inputs and add them to input, iff this has
        # not happened in any connector piece in this pipeline before.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultModuleToEnv
        ):
            self.append(DefaultModuleToEnv(
                input_observation_space=self.observation_space,
                input_action_space=self.action_space,
                env=env,
                rl_module=rl_module,
            ))


class LearnerPipeline(ConnectorPipelineV2):
    def __init__(
        self,
        *,
        connectors: Optional[List[ConnectorV2]] = None,
        input_observation_space: Optional[gym.Space],
        input_action_space: Optional[gym.Space],
        env: Optional[gym.Env] = None,
        rl_module: Optional["RLModule"] = None,
        **kwargs,
    ):
        super().__init__(
            connectors=connectors,
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            env=env,
            rl_module=rl_module,
            **kwargs,
        )

        # Add the default final connector piece for learner pipelines:
        # Making sure that we have - at the minimum - observations and that the data
        # is time-ranked (if we have a stateful model) and properly zero-padded.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultLearnerConnector
        ):
            self.append(DefaultLearnerConnector(
                input_observation_space=self.observation_space,
                input_action_space=self.action_space,
            ))
