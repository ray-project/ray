from collections import defaultdict
import logging
import pathlib
import pickle
import pyarrow.fs
from typing import Any, Collection, Dict, List, Optional, Tuple, Type, Union

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.checkpoints import Checkpointable, _exists_at_fs_path
from ray.rllib.utils.typing import EpisodeType, StateDict
from ray.util.annotations import PublicAPI
from ray.util.timer import _Timer

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class ConnectorPipelineV2(ConnectorV2):
    """Utility class for quick manipulation of a connector pipeline."""

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        self._fix_spaces(input_observation_space, input_action_space)
        return self.observation_space

    @override(ConnectorV2)
    def recompute_output_action_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        self._fix_spaces(input_observation_space, input_action_space)
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
        if connectors:
            if all(isinstance(conn, type) for conn in connectors):
                self.connectors = connectors
                self.connector_class_names = [
                    conn.__class__.__name__ for conn in connectors
                ]
            elif all(isinstance(conn, str) for conn in connectors):
                self.connectors = []
                self.connector_class_names = connectors
        else:
            self.connectors = []

        super().__init__(input_observation_space, input_action_space, **kwargs)

        self.timers = defaultdict(_Timer)

    def __len__(self):
        return len(self.connectors)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
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
                batch = connector(
                    rl_module=rl_module,
                    batch=batch,
                    episodes=episodes,
                    explore=explore,
                    shared_data=shared_data,
                    # Deprecated arg.
                    data=batch,
                    **kwargs,
                )
                if not isinstance(batch, dict):
                    raise ValueError(
                        f"`data` returned by ConnectorV2 {connector} must be a dict! "
                        f"You returned {batch}. Check your (custom) connectors' "
                        f"`__call__()` method's return value and make sure you return "
                        f"the `data` arg passed in (either altered or unchanged)."
                    )
        return batch

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
            self._fix_spaces(self.input_observation_space, self.input_action_space)
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
        self._fix_spaces(self.input_observation_space, self.input_action_space)

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
        self._fix_spaces(self.input_observation_space, self.input_action_space)

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
        self._fix_spaces(self.input_observation_space, self.input_action_space)

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
        self._fix_spaces(self.input_observation_space, self.input_action_space)

        logger.info(
            f"Added {connector.__class__.__name__} to the end of "
            f"{self.__class__.__name__}."
        )

    @override(ConnectorV2)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        state = {}
        for conn in self.connectors:
            conn_name = type(conn).__name__
            if self._check_component(conn_name, components, not_components):
                state[conn_name] = conn.get_state(
                    components=self._get_subcomponents(conn_name, components),
                    not_components=self._get_subcomponents(conn_name, not_components),
                    **kwargs,
                )
        return state

    @override(ConnectorV2)
    def set_state(self, state: Dict[str, Any]) -> None:
        for conn in self.connectors:
            conn_name = type(conn).__name__
            if conn_name in state:
                conn.set_state(state[conn_name])

    @override(Checkpointable)
    def get_checkpointable_components(self) -> List[Tuple[str, "Checkpointable"]]:
        if self.connectors:
            return [(type(conn).__name__, conn) for conn in self.connectors]
        elif self.connector_class_names:
            return [(conn_name, None) for conn_name in self.connector_class_names]

    # Note that we don't have to override Checkpointable.get_ctor_args_and_kwargs and
    # don't have to return the `connectors` c'tor kwarg from there. This is b/c all
    # connector pieces in this pipeline are themselves Checkpointable components,
    # so they will be properly written into this pipeline's checkpoint.
    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self) -> Tuple[Tuple, Dict[str, Any]]:
        return (
            (self.input_observation_space, self.input_action_space),  # *args
            {
                "connectors": [conn.__class__.__name__ for conn in self.connectors]
            },  # **kwargs
        )

    @override(Checkpointable)
    def restore_from_path(
        self,
        path: Union[str, pathlib.Path],
        *,
        component: Optional[str] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        **kwargs,
    ) -> None:
        """Restores the state of the implementing class from the given path.

        If the `component` arg is provided, `path` refers to a checkpoint of a
        subcomponent of `self`, thus allowing the user to load only the subcomponent's
        state into `self` without affecting any of the other state information (for
        example, loading only the NN state into a Checkpointable, which contains such
        an NN, but also has other state information that should NOT be changed by
        calling this method).

        The given `path` should have the following structure and contain the following
        files:

        .. testcode::
            :skipif: True

            path/
                [component1]/
                    [component1 subcomponentA]/
                        ...
                    [component1 subcomponentB]/
                        ...
                [component2]/
                        ...
                [cls.METADATA_FILE_NAME] (json)
                [cls.STATE_FILE_NAME] (pkl)

        Note that the self.METADATA_FILE_NAME file is not required to restore the state.

        Args:
            path: The path to load the implementing class' state from or to load the
                state of only one subcomponent's state of the implementing class (if
                `component` is provided).
            component: If provided, `path` is interpreted as the checkpoint path of only
                the subcomponent and thus, only that subcomponent's state is
                restored/loaded. All other state of `self` remains unchanged in this
                case.
            filesystem: PyArrow FileSystem to use to access data at the `path`. If not
                specified, this is inferred from the URI scheme of `path`.
            **kwargs: Forward compatibility kwargs.
        """
        path = path if isinstance(path, str) else path.as_posix()

        if path and not filesystem:
            # Note the path needs to be a path that is relative to the
            # filesystem (e.g. `gs://tmp/...` -> `tmp/...`).
            filesystem, path = pyarrow.fs.FileSystem.from_uri(path)
        # Only here convert to a `Path` instance b/c otherwise
        # cloud path gets broken (i.e. 'gs://' -> 'gs:/').
        path = pathlib.Path(path)

        if not _exists_at_fs_path(filesystem, path.as_posix()):
            raise FileNotFoundError(f"`path` ({path}) not found!")

        # Restore components of `self` that themselves are `Checkpointable`.
        for comp_name, comp in self.get_checkpointable_components():

            # The value of the `component` argument for the upcoming
            # `[subcomponent].restore_from_path(.., component=..)` call.
            comp_arg = None

            if component is None:
                comp_dir = path / comp_name
                # If subcomponent's dir is not in path, ignore it and don't restore this
                # subcomponent's state from disk.
                if not _exists_at_fs_path(filesystem, comp_dir.as_posix()):
                    continue
            else:
                comp_dir = path

                # `component` is a path that starts with `comp` -> Remove the name of
                # `comp` from the `component` arg in the upcoming call to `restore_..`.
                if component.startswith(comp_name + "/"):
                    comp_arg = component[len(comp_name) + 1 :]
                # `component` has nothing to do with `comp` -> Skip.
                elif component != comp_name:
                    continue

            # If component is an ActorManager, restore all the manager's healthy
            # actors' states from disk (even if they are on another node, in which case,
            # we'll sync checkpoint file(s) to the respective node).
            if comp is None:
                # Get the class constructor to call.
                with filesystem.open_input_stream(
                    (comp_dir / self.CLASS_AND_CTOR_ARGS_FILE_NAME).as_posix()
                ) as f:
                    comp_ctor_info = pickle.load(f)
                comp_ctor = comp_ctor_info["class"]

                # Check, whether the constructor actually goes together with `cls`.
                if not issubclass(comp_ctor, ConnectorV2):
                    raise ValueError(
                        f"The class ({comp_ctor}) stored in checkpoint ({path}) does "
                        f"not seem to be a subclass of `cls` ({ConnectorV2})!"
                    )
                elif not issubclass(comp_ctor, Checkpointable):
                    raise ValueError(
                        f"The class ({comp_ctor}) stored in checkpoint ({path}) does "
                        "not seem to be an implementer of the `Checkpointable` API!"
                    )

                comp = comp_ctor(
                    *comp_ctor_info["ctor_args_and_kwargs"][0],
                    **comp_ctor_info["ctor_args_and_kwargs"][1],
                )

            # Call `restore_from_path()` on local subcomponent, thereby passing in the
            # **kwargs.
            comp.restore_from_path(
                comp_dir, filesystem=filesystem, component=comp_arg, **kwargs
            )
            self.append(comp)

        # Restore the rest of the state (not based on subcomponents).
        if component is None:
            with filesystem.open_input_stream(
                (path / self.STATE_FILE_NAME).as_posix()
            ) as f:
                state = pickle.load(f)
            self.set_state(state)

    @override(ConnectorV2)
    def reset_state(self) -> None:
        for conn in self.connectors:
            conn.reset_state()

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

    def _fix_spaces(self, input_observation_space, input_action_space):
        if len(self) > 0:
            # Fix each connector's input_observation- and input_action space in
            # the pipeline.
            obs_space = input_observation_space
            act_space = input_action_space
            for con in self.connectors:
                con.input_action_space = act_space
                con.input_observation_space = obs_space
                obs_space = con.observation_space
                act_space = con.action_space
