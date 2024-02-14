import abc
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional, Union

import gymnasium as gym

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.typing import EpisodeType, ModuleID
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ConnectorV2(abc.ABC):
    """Base class defining the API for an individual "connector piece".

    A ConnectorV2 ("connector piece") is usually part of a whole series of connector
    pieces within a so-called connector pipeline, which in itself also abides to this
    very API..
    For example, you might have a connector pipeline consisting of two connector pieces,
    A and B, both instances of subclasses of ConnectorV2 and each one performing a
    particular transformation on their input data. The resulting connector pipeline
    (A->B) itself also abides to this very ConnectorV2 API and could thus be part of yet
    another, higher-level connector pipeline.

    Any ConnectorV2 instance (individual pieces or several connector pieces in a
    pipeline) is a callable and you should override their `__call__()` method.
    When called, they take the outputs of a previous connector piece (or an empty dict
    if there are no previous pieces) as well as all the data collected thus far in the
    ongoing episode(s) (only applies to connectors used in EnvRunners) or retrieved
    from a replay buffer or from an environment sampling step (only applies to
    connectors used in Learner pipelines). From this input data, a ConnectorV2 then
    performs a transformation step.

    There are 3 types of pipelines any ConnectorV2 piece can belong to:
    1) EnvToModulePipeline: The connector transforms environment data before it gets to
    the RLModule. This type of pipeline is used by an EnvRunner for transforming
    env output data into RLModule readable data (for the next RLModule forward pass).
    For example, such a pipeline would include observation postprocessors, -filters,
    or any RNN preparation code related to time-sequences and zero-padding.
    2) ModuleToEnvPipeline: This type of pipeline is used by an
    EnvRunner to transform RLModule output data to env readable actions (for the next
    `env.step()` call). For example, in case the RLModule only outputs action
    distribution parameters (but not actual actions), the ModuleToEnvPipeline would
    take care of sampling the actions to be sent back to the end from the
    resulting distribution (made deterministic if exploration is off).
    3) LearnerConnectorPipeline: This connector pipeline type transforms data coming
    from an `EnvRunner.sample()` call or a replay buffer and will then be sent into the
    RLModule's `forward_train()` method in order to compute loss function inputs.
    This type of pipeline is used by a Learner worker to transform raw training data
    (a batch or a list of episodes) to RLModule readable training data (for the next
    RLModule `forward_train()` call).

    Some connectors might be stateful, for example for keeping track of observation
    filtering stats (mean and stddev values). Any Algorithm, which uses connectors is
    responsible for frequently synchronizing the states of all connectors and connector
    pipelines between the EnvRunners (owning the env-to-module and module-to-env
    pipelines) and the Learners (owning the Learner pipelines).
    """

    @property
    def observation_space(self):
        """Getter for our (output) observation space.

        Logic: Use user provided space (if set via `observation_space` setter)
        otherwise, use the same as the input space, assuming this connector piece
        does not alter the space.
        """
        return self.input_observation_space

    @property
    def action_space(self):
        """Getter for our (output) action space.

        Logic: Use user provided space (if set via `action_space` setter)
        otherwise, use the same as the input space, assuming this connector piece
        does not alter the space.
        """
        return self.input_action_space

    def __init__(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        **kwargs,
    ):
        """Initializes a ConnectorV2 instance.

        Args:
            input_observation_space: The (optional) input observation space for this
                connector piece. This is the space coming from a previous connector
                piece in the (env-to-module or learner) pipeline or is directly
                defined within the gym.Env.
            input_action_space: The (optional) input action space for this connector
                piece. This is the space coming from a previous connector piece in the
                (module-to-env) pipeline or is directly defined within the gym.Env.
            **kwargs: Forward API-compatibility kwargs.
        """
        self.input_observation_space = input_observation_space
        self.input_action_space = input_action_space

    @abc.abstractmethod
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
        """Method for transforming input data into output data.

        Args:
            rl_module: The RLModule object that the connector connects to or from.
            data: The input data to be transformed by this connector. Transformations
                might either be done in-place or a new structure may be returned.
            episodes: The list of SingleAgentEpisode or MultiAgentEpisode objects,
                each corresponding to one slot in the vector env. Note that episodes
                should always be considered read-only and not be altered.
            explore: Whether `explore` is currently on. Per convention, if True, the
                RLModule's `forward_exploration` method should be called, if False, the
                EnvRunner should call `forward_inference` instead.
            shared_data: Optional additional context data that needs to be exchanged
                between different Connector pieces and -pipelines.
            kwargs: Forward API-compatibility kwargs.

        Returns:
            The transformed connector output.
        """

    @staticmethod
    def single_agent_episode_iterator(
        episodes: List[EpisodeType],
        agents_that_stepped_only: bool = True,
        zip_with_batch_column: Optional[Union[List[Any], Dict[tuple, Any]]] = None,
    ) -> Iterator[SingleAgentEpisode]:
        """An iterator over a list of episodes yielding always SingleAgentEpisodes.

        In case items in the list are MultiAgentEpisodes, these are broken down
        into their individual agents' SingleAgentEpisodes and those are then yielded
        one after the other.

        Useful for connectors that operate on both single-agent and multi-agent
        episodes.

        Args:
            episodes: The list of SingleAgent- or MultiAgentEpisode objects.
            agents_that_stepped_only: If True (and multi-agent setup), will only place
                items of those agents into the batch that have just stepped in the
                actual MultiAgentEpisode (this is checked via a
                `MultiAgentEpside.episode.get_agents_to_act()`). Note that this setting
                is ignored in a single-agent setups b/c the agent steps at each timestep
                regardless.
            zip_with_batch_column: If provided, must be a list of batch items
                corresponding to the given `episodes` (single agent case) or a dict
                mapping (AgentID, ModuleID) tuples to lists of individual batch items
                corresponding to this agent/module combination. The iterator will then
                yield tuples of SingleAgentEpisode objects (1st item) along with the
                data item (2nd item) that this episode was responsible for generating
                originally.

        Yields:
            All SingleAgentEpisodes in the input list, whereby MultiAgentEpisodes will
            be broken down into their individual SingleAgentEpisode components.
        """
        # Single-agent case.
        if isinstance(episodes[0], SingleAgentEpisode):
            if zip_with_batch_column is not None:
                if len(zip_with_batch_column) != len(episodes):
                    raise ValueError(
                        "Invalid `zip_with_batch_column` data: Must have the same "
                        f"length as the list of episodes ({len(episodes)}), but has "
                        f"length {len(zip_with_batch_column)}!"
                    )
                for episode, data in zip(episodes, zip_with_batch_column):
                    yield episode, data
            else:
                for episode in episodes:
                    yield episode
            return

        # Multi-agent case.
        list_indices = defaultdict(int)
        for episode in episodes:
            agent_ids = (
                episode.get_agents_that_stepped()
                if agents_that_stepped_only
                else episode.agent_ids
            )
            for agent_id in agent_ids:
                sa_episode = episode.agent_episodes[agent_id]
                # for sa_episode in episode.agent_episodes.values():
                if zip_with_batch_column is not None:
                    key = (sa_episode.agent_id, sa_episode.module_id)
                    if len(zip_with_batch_column[key]) <= list_indices[key]:
                        raise ValueError(
                            "Invalid `zip_with_batch_column` data: Must structurally "
                            "match the single-agent contents in the given list of "
                            "(multi-agent) episodes!"
                        )
                    d = zip_with_batch_column[key][list_indices[key]]
                    list_indices[key] += 1
                    yield sa_episode, d
                else:
                    yield sa_episode

    @staticmethod
    def add_batch_item(
        batch: Dict[str, Any],
        column: str,
        item_to_add: Any,
        single_agent_episode: Optional[SingleAgentEpisode] = None,
    ) -> None:
        """Adds a data item under `column` to the given `batch`.

        If `single_agent_episode` is provided and it contains agent ID and module ID
        information, will store the item in a list under a `([agent_id],[module_id])`
        key within `column`. In all other cases, will store the item in a list directly
        under `column`.

        .. testcode::

            from ray.rllib.connectors.connector_v2 import ConnectorV2
            from ray.rllib.env.single_agent_episode import SingleAgentEpisode
            from ray.rllib.utils.test_utils import check

            batch = {}
            ConnectorV2.add_batch_item(batch, "test_col", 5)

            check(batch, {"test_col": [5]})

            sa_episode = SingleAgentEpisode(agent_id="ag1", module_id="module_10")
            ConnectorV2.add_batch_item(batch, "test_col_2", -10, sa_episode)

            check(batch, {
                "test_col": [5],
                "test_col_2": {
                    ("ag1", "module_10"): [-10],
                },
            })

        Args:
            batch: The batch to store `item_to_add` in.
            column: The column name (str) within the `batch` to store `item_to_add`
                under.
            item_to_add: The data item to store in the batch.
            single_agent_episode: An optional SingleAgentEpisode. If provided and its
                agent_id and module_id properties are not None, will create a further
                sub dictionary under `column`, mapping from `([agent_id],[module_id])`
                (str) to a list of data items. Otherwise, will store `item_to_add`
                in a list directly under `column`.
        """
        sub_key = None
        if (
            single_agent_episode is not None
            and single_agent_episode.agent_id is not None
        ):
            sub_key = (single_agent_episode.agent_id, single_agent_episode.module_id)

        if column not in batch:
            batch[column] = [] if sub_key is None else {sub_key: []}
        if sub_key:
            if sub_key not in batch[column]:
                batch[column][sub_key] = []
            batch[column][sub_key].append(item_to_add)
        else:
            batch[column].append(item_to_add)

    @staticmethod
    def foreach_batch_item_change_in_place(batch, column: str, func) -> None:
        data_to_process = batch.get(column)

        if not data_to_process:
            raise ValueError(
                f"Invalid column name ({column})! Not found in given batch."
            )

        # Single-agent case: There is a list of individual observation items directly
        # under the "obs" key. AgentID and ModuleID are both None.
        if isinstance(data_to_process, list):
            for i, d in enumerate(data_to_process):
                data_to_process[i] = func(d, None, None)
        # Multi-agent case: There is a dict mapping from a (AgentID, ModuleID) tuples to
        # lists of individual data items.
        else:
            for (agent_id, module_id), d_list in data_to_process.items():
                for i, d in enumerate(d_list):
                    data_to_process[(agent_id, module_id)][i] = func(
                        d, agent_id, module_id
                    )

    @staticmethod
    def switch_batch_from_column_to_module_ids(
        batch: Dict[str, Dict[ModuleID, Any]]
    ) -> Dict[ModuleID, Dict[str, Any]]:
        """Switches the first two levels of a `col -> ModuleID -> data` type batch.

        Assuming that the top level consists of column names as keys and the second
        level (under these columns) consists of ModuleID keys, the resulting batch
        will have these two reversed and thus map ModuleIDs to dicts mapping column
        names to data items.

        .. testcode::

            from ray.rllib.utils.test_utils import check

            batch = {
                "obs": {"module_0": [1, 2, 3]},
                "actions": {"module_0": [4, 5, 6], "module_1": [7]},
            }
            switched_batch = ConnectorV2.switch_batch_from_column_to_module_ids(batch)
            check(
                switched_batch,
                {
                    "module_0": {"obs": [1, 2, 3], "actions": [4, 5, 6]},
                    "module_1": {"actions": [7]},
                },
            )

        Args:
            batch: The batch to switch from being column name based (then ModuleIDs)
                to being ModuleID based (then column names).

        Returns:
            A new batch dict mapping ModuleIDs to dicts mapping column names (e.g.
            "obs") to data.
        """
        module_data = defaultdict(dict)
        for column, column_data in batch.items():
            for module_id, data in column_data.items():
                module_data[module_id][column] = data
        return dict(module_data)

    def get_state(self) -> Dict[str, Any]:
        """Returns the current state of this ConnectorV2 as a state dict.

        Returns:
            A state dict mapping any string keys to their (state-defining) values.
        """
        return {}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of this ConnectorV2 to the given value.

        Args:
            state: The state dict to define this ConnectorV2's new state.
        """
        pass

    def reset_state(self) -> None:
        """Resets the state of this ConnectorV2 to some initial value.

        Note that this may NOT be the exact state that this ConnectorV2 was originally
        constructed with.
        """
        pass

    @staticmethod
    def merge_states(states: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Computes a resulting state given a list of other state dicts.

        Algorithms should use this method for synchronizing states between connectors
        running on workers (of the same type, e.g. EnvRunner workers).

        Args:
            states: The list of n other ConnectorV2 states to merge into a single
                resulting state.

        Returns:
            The resulting state dict.
        """
        return {}

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__
