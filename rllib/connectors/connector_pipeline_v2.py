from collections import defaultdict
import logging
from typing import Any, Dict, List, Optional, Type, Union

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI
from ray.util.timer import _Timer

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class ConnectorPipelineV2(ConnectorV2):
    """Utility class for quick manipulation of a connector pipeline."""

    @override(ConnectorV2)
    def recompute_observation_space_from_input_spaces(self):
        self._fix_spaces()
        return self.observation_space

    @override(ConnectorV2)
    def recompute_action_space_from_input_spaces(self):
        self._fix_spaces()
        return self.action_space

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        connectors: Optional[List[ConnectorV2]] = None,
        **kwargs,
    ):
        """Initializes a ConnectorPipelineV2 instance.

        Args:
            input_observation_space: The (optional) input observation space for this
                connector piece. This is the space coming from a previous connector
                piece in the (env-to-module or learner) pipeline or is directly
                defined within the gym.Env.
            input_action_space: The (optional) input action space for this connector
                piece. This is the space coming from a previous connector piece in the
                (module-to-env) pipeline or is directly defined within the gym.Env.
            connectors: A list of individual ConnectorV2 pieces to be added to this
                pipeline during construction. Note that you can always add (or remove)
                more ConnectorV2 pieces later on the fly.
        """
        self.connectors = connectors or []

        super().__init__(input_observation_space, input_action_space, **kwargs)

        self.timers = defaultdict(_Timer)

    def __len__(self):
        return len(self.connectors)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        """In a pipeline, we simply call each of our connector pieces after each other.

        Each connector piece receives as input the output of the previous connector
        piece in the pipeline.
        """
        shared_data = shared_data if shared_data is not None else {}
        # Loop through connector pieces and call each one with the output of the
        # previous one. Thereby, time each connector piece's call.
        for connector in self.connectors:
            timer = self.timers[str(connector)]
            with timer:
                data = connector(
                    rl_module=rl_module,
                    data=data,
                    episodes=episodes,
                    explore=explore,
                    shared_data=shared_data,
                    **kwargs,
                )
        return data

    def remove(self, name_or_class: Union[str, Type]):
        """Remove a single connector piece in this pipeline by its name or class.

        Args:
            name: The name of the connector piece to be removed from the pipeline.
        """
        idx = -1
        for i, c in enumerate(self.connectors):
            if c.__class__.__name__ == name_or_class:
                idx = i
                break
        if idx >= 0:
            del self.connectors[idx]
            self._fix_spaces()
            logger.info(
                f"Removed connector {name_or_class} from {self.__class__.__name__}."
            )
        else:
            logger.warning(
                f"Trying to remove a non-existent connector {name_or_class}."
            )

    def insert_before(
        self,
        name_or_class: Union[str, type],
        connector: ConnectorV2,
    ) -> ConnectorV2:
        """Insert a new connector piece before an existing piece (by name or class).

        Args:
            name_or_class: Name or class of the connector piece before which `connector`
                will get inserted.
            connector: The new connector piece to be inserted.

        Returns:
            The ConnectorV2 before which `connector` has been inserted.
        """
        idx = -1
        for idx, c in enumerate(self.connectors):
            if (
                isinstance(name_or_class, str) and c.__class__.__name__ == name_or_class
            ) or (isinstance(name_or_class, type) and c.__class__ is name_or_class):
                break
        if idx < 0:
            raise ValueError(
                f"Can not find connector with name or type '{name_or_class}'!"
            )
        next_connector = self.connectors[idx]

        self.connectors.insert(idx, connector)
        self._fix_spaces()

        logger.info(
            f"Inserted {connector.__class__.__name__} before {name_or_class} "
            f"to {self.__class__.__name__}."
        )
        return next_connector

    def insert_after(
        self,
        name_or_class: Union[str, Type],
        connector: ConnectorV2,
    ) -> ConnectorV2:
        """Insert a new connector piece after an existing piece (by name or class).

        Args:
            name_or_class: Name or class of the connector piece after which `connector`
                will get inserted.
            connector: The new connector piece to be inserted.

        Returns:
            The ConnectorV2 after which `connector` has been inserted.
        """
        idx = -1
        for idx, c in enumerate(self.connectors):
            if (
                isinstance(name_or_class, str) and c.__class__.__name__ == name_or_class
            ) or (isinstance(name_or_class, type) and c.__class__ is name_or_class):
                break
        if idx < 0:
            raise ValueError(
                f"Can not find connector with name or type '{name_or_class}'!"
            )
        prev_connector = self.connectors[idx]

        self.connectors.insert(idx + 1, connector)
        self._fix_spaces()

        logger.info(
            f"Inserted {connector.__class__.__name__} after {name_or_class} "
            f"to {self.__class__.__name__}."
        )

        return prev_connector

    def prepend(self, connector: ConnectorV2) -> None:
        """Prepend a new connector at the beginning of a connector pipeline.

        Args:
            connector: The new connector piece to be prepended to this pipeline.
        """
        self.connectors.insert(0, connector)
        self._fix_spaces()

        logger.info(
            f"Added {connector.__class__.__name__} to the beginning of "
            f"{self.__class__.__name__}."
        )

    def append(self, connector: ConnectorV2) -> None:
        """Append a new connector at the end of a connector pipeline.

        Args:
            connector: The new connector piece to be appended to this pipeline.
        """
        self.connectors.append(connector)
        self._fix_spaces()

        logger.info(
            f"Added {connector.__class__.__name__} to the end of "
            f"{self.__class__.__name__}."
        )

    @override(ConnectorV2)
    def get_state(self) -> Dict[str, Any]:
        states = {}
        for i, connector in enumerate(self.connectors):
            key = f"{i:03d}_{type(connector).__name__}"
            state = connector.get_state()
            states[key] = state
        return states

    @override(ConnectorV2)
    def set_state(self, state: Dict[str, Any]) -> None:
        for i, connector in enumerate(self.connectors):
            key = f"{i:03d}_{type(connector).__name__}"
            if key not in state:
                raise KeyError(f"No state found in `state` for connector piece: {key}!")
            connector.set_state(state[key])

    @override(ConnectorV2)
    def merge_states(self, states: List[Dict[str, Any]]) -> Dict[str, Any]:
        merged_states = {}
        if not states:
            return merged_states
        for i, (key, item) in enumerate(states[0].items()):
            state_list = [state[key] for state in states]
            conn = self.connectors[i]
            merged_states[key] = conn.merge_states(state_list)
        return merged_states

    def __repr__(self, indentation: int = 0):
        return "\n".join(
            [" " * indentation + self.__class__.__name__]
            + [c.__str__(indentation + 4) for c in self.connectors]
        )

    def __getitem__(
        self,
        key: Union[str, int, Type],
    ) -> Union[ConnectorV2, List[ConnectorV2]]:
        """Returns a single ConnectorV2 or list of ConnectorV2s that fit `key`.

        If key is an int, we return a single ConnectorV2 at that index in this pipeline.
        If key is a ConnectorV2 type or a string matching the class name of a
        ConnectorV2 in this pipeline, we return a list of all ConnectorV2s in this
        pipeline matching the specified class.

        Args:
            key: The key to find or to index by.

        Returns:
            A single ConnectorV2 or a list of ConnectorV2s matching `key`.
        """
        # Key is an int -> Index into pipeline and return.
        if isinstance(key, int):
            return self.connectors[key]
        # Key is a class.
        elif isinstance(key, type):
            results = []
            for c in self.connectors:
                if issubclass(c.__class__, key):
                    results.append(c)
            return results
        # Key is a string -> Find connector(s) by name.
        elif isinstance(key, str):
            results = []
            for c in self.connectors:
                if c.name == key:
                    results.append(c)
            return results
        # Slicing not supported (yet).
        elif isinstance(key, slice):
            raise NotImplementedError(
                "Slicing of ConnectorPipelineV2 is currently not supported!"
            )
        else:
            raise NotImplementedError(
                f"Indexing ConnectorPipelineV2 by {type(key)} is currently not "
                f"supported!"
            )

    @property
    def observation_space(self):
        if len(self) > 0:
            return self.connectors[-1].observation_space
        return self._observation_space

    @property
    def action_space(self):
        if len(self) > 0:
            return self.connectors[-1].action_space
        return self._action_space

    def _fix_spaces(self):
        if len(self) > 0:
            # Fix each connector's input_observation- and input_action space in
            # the pipeline.
            obs_space = self.input_observation_space
            act_space = self.input_action_space
            for con in self.connectors:
                con.input_action_space = act_space
                con.input_observation_space = obs_space
                obs_space = con.observation_space
                act_space = con.action_space
