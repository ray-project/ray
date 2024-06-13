import abc
from collections import defaultdict
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import gymnasium as gym
import tree

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic
from ray.rllib.utils.spaces.space_utils import BatchedNdArray
from ray.rllib.utils.typing import AgentID, EpisodeType, ModuleID
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
    another, higher-level connector pipeline, e.g. (A->B)->C->D.

    Any ConnectorV2 instance (individual pieces or several connector pieces in a
    pipeline) is a callable and users should override the `__call__()` method.
    When called, they take the outputs of a previous connector piece (or an empty dict
    if there are no previous pieces) and all the data collected thus far in the
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

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
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
        self._observation_space = None
        self._action_space = None
        self._input_observation_space = None
        self._input_action_space = None
        self.input_observation_space = input_observation_space
        self.input_action_space = input_action_space

    @OverrideToImplementCustomLogic
    def recompute_observation_space_from_input_spaces(self) -> gym.Space:
        """Re-computes a new (output) observation space based on the input space.

        This method should be overridden by users to make sure a ConnectorPipelineV2
        knows how the input spaces through its individual ConnectorV2 pieces are being
        transformed.

        .. testcode::

            from gymnasium.spaces import Box, Discrete
            import numpy as np

            from ray.rllib.connectors.connector_v2 import ConnectorV2
            from ray.rllib.utils.numpy import one_hot
            from ray.rllib.utils.test_utils import check

            class OneHotConnector(ConnectorV2):
                def recompute_observation_space_from_input_spaces(self):
                    return Box(0.0, 1.0, (self.input_observation_space.n,), np.float32)

                def __call__(
                    self,
                    *,
                    rl_module,
                    data,
                    episodes,
                    explore=None,
                    shared_data=None,
                    **kwargs,
                ):
                    assert "obs" in data
                    data["obs"] = one_hot(data["obs"])
                    return data

            connector = OneHotConnector(input_observation_space=Discrete(2))
            data = {"obs": np.array([1, 0, 0], np.int32)}
            output = connector(rl_module=None, data=data, episodes=None)

            check(output, {"obs": np.array([[0.0, 1.0], [1.0, 0.0], [1.0, 0.0]])})

        If this ConnectorV2 does not change the observation space in any way, leave
        this parent method implementation untouched.

        Returns:
            The new observation space (after data has passed through this ConnectorV2
            piece).
        """
        return self.input_observation_space

    @OverrideToImplementCustomLogic
    def recompute_action_space_from_input_spaces(self) -> gym.Space:
        """Re-computes a new (output) action space based on the input space.

        This method should be overridden by users to make sure a ConnectorPipelineV2
        knows how the input spaces through its individual ConnectorV2 pieces are being
        transformed.

        If this ConnectorV2 does not change the action space in any way, leave
        this parent method implementation untouched.

        Returns:
            The new action space (after data has passed through this ConenctorV2
            piece).
        """
        return self.input_action_space

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
                Note that the information in `data` will eventually either become the
                forward batch for the RLModule (env-to-module and learner connectors)
                or the input to the `env.step()` call (module-to-env connectors). In
                the former case (`data` is a forward batch for RLModule), the
                information in `data` will be discarded after the RLModule forward pass.
                Any transformation of information (e.g. observation preprocessing) that
                you have only done inside `data` will be lost, unless you have written
                it back into the corresponding `episodes` during the connector pass.
            episodes: The list of SingleAgentEpisode or MultiAgentEpisode objects,
                each corresponding to one slot in the vector env. Note that episodes
                can be read from (e.g. to place information into `data`), but also
                written to. You should only write back (changed, transformed)
                information into the episodes, if you want these changes to be
                "permanent". For example if you sample from an environment, pick up
                observations from the episodes and place them into `data`, then
                transform these observations, and would like to make these
                transformations permanent (note that `data` gets discarded after the
                RLModule forward pass), then you have to write the transformed
                observations back into the episode to make sure you do not have to
                perform the same transformation again on the learner (or replay buffer)
                side. The Learner will hence work on the already changed episodes (and
                compile the train batch using the Learner connector).
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
        zip_with_batch_column: Optional[Union[List[Any], Dict[Tuple, Any]]] = None,
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
        list_indices = defaultdict(int)

        # Single-agent case.
        if isinstance(episodes[0], SingleAgentEpisode):
            if zip_with_batch_column is not None:
                if len(zip_with_batch_column) != len(episodes):
                    raise ValueError(
                        "Invalid `zip_with_batch_column` data: Must have the same "
                        f"length as the list of episodes ({len(episodes)}), but has "
                        f"length {len(zip_with_batch_column)}!"
                    )
                # Simple case: Items are stored in lists directly under the column (str)
                # key.
                if isinstance(zip_with_batch_column, list):
                    for episode, data in zip(episodes, zip_with_batch_column):
                        yield episode, data
                # Normal single-agent case: Items are stored in dicts under the column
                # (str) key. These dicts map (eps_id,)-tuples to lists of individual
                # items.
                else:
                    for episode, (eps_id_tuple, data) in zip(
                        episodes,
                        zip_with_batch_column.items(),
                    ):
                        assert episode.id_ == eps_id_tuple[0]
                        d = data[list_indices[eps_id_tuple]]
                        list_indices[eps_id_tuple] += 1
                        yield episode, d
            else:
                for episode in episodes:
                    yield episode
            return

        # Multi-agent case.
        for episode in episodes:
            for agent_id in (
                episode.get_agents_that_stepped()
                if agents_that_stepped_only
                else episode.agent_ids
            ):
                sa_episode = episode.agent_episodes[agent_id]
                # for sa_episode in episode.agent_episodes.values():
                if zip_with_batch_column is not None:
                    key = (
                        sa_episode.multi_agent_episode_id,
                        sa_episode.agent_id,
                        sa_episode.module_id,
                    )
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

        If `single_agent_episode` is provided and contains `agent_id` and `module_id`
        information, will store `item_to_add` in a list under a
        `([eps id], [AgentID],[ModuleID])` key within `column`. In all other
        cases, will store the item in a list directly under `column`.

        .. testcode::

            from ray.rllib.connectors.connector_v2 import ConnectorV2
            from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
            from ray.rllib.env.single_agent_episode import SingleAgentEpisode
            from ray.rllib.utils.test_utils import check

            # Simple case (no episodes provided) -> Store data in a list directly under
            # `column`:
            batch = {}
            ConnectorV2.add_batch_item(batch, "test_col", item_to_add=5)
            ConnectorV2.add_batch_item(batch, "test_col", item_to_add=6)
            check(batch, {"test_col": [5, 6]})
            ConnectorV2.add_batch_item(batch, "test_col_2", item_to_add=-10)
            check(batch, {
                "test_col": [5, 6],
                "test_col_2": [-10],
            })

            # Single-agent case (SingleAgentEpisode provided) -> Store data in a list
            # under the keys: `column` -> `(eps_id,)`:
            batch = {}
            episode = SingleAgentEpisode(
                id_="SA-EPS0",
                observations=[0, 1, 2, 3],
                actions=[1, 2, 3],
                rewards=[1.0, 2.0, 3.0],
            )
            ConnectorV2.add_batch_item(batch, "test_col", 5, episode)
            ConnectorV2.add_batch_item(batch, "test_col", 6, episode)
            ConnectorV2.add_batch_item(batch, "test_col_2", -10, episode)
            check(batch, {
                "test_col": {("SA-EPS0",): [5, 6]},
                "test_col_2": {("SA-EPS0",): [-10]},
            })

            # Multi-agent case (SingleAgentEpisode provided that has `agent_id` and
            # `module_id` information) -> Store data in a list under the keys:
            # `column` -> `([eps_id], [agent_id], [module_id])`:
            batch = {}
            ma_episode = MultiAgentEpisode(
                id_="MA-EPS1",
                observations=[
                    {"ag0": 0, "ag1": 1}, {"ag0": 2, "ag1": 4}
                ],
                actions=[{"ag0": 0, "ag1": 1}],
                rewards=[{"ag0": -0.1, "ag1": -0.2}],
                # ag0 maps to mod0, ag1 maps to mod1, etc..
                agent_to_module_mapping_fn=lambda aid, eps: f"mod{aid[2:]}",
            )
            ConnectorV2.add_batch_item(
                batch,
                "test_col",
                item_to_add=5,
                single_agent_episode=ma_episode.agent_episodes["ag0"],
            )
            ConnectorV2.add_batch_item(
                batch,
                "test_col",
                item_to_add=6,
                single_agent_episode=ma_episode.agent_episodes["ag0"],
            )
            ConnectorV2.add_batch_item(
                batch,
                "test_col_2",
                item_to_add=10,
                single_agent_episode=ma_episode.agent_episodes["ag1"],
            )
            check(
                batch,
                {
                    "test_col": {("MA-EPS1", "ag0", "mod0"): [5, 6]},
                    "test_col_2": {("MA-EPS1", "ag1", "mod1"): [10]},
                },
            )

        Args:
            batch: The batch to store `item_to_add` in.
            column: The column name (str) within the `batch` to store `item_to_add`
                under.
            item_to_add: The data item to store in the batch.
            single_agent_episode: An optional SingleAgentEpisode. If provided and its
                `agent_id` and `module_id` properties are not None, will create a
                further sub dictionary under `column`, mapping from
                `([eps id], [agent_id], [module_id])` to a list of
                data items (to which `item_to_add` will be appended in this call).
                If not provided, will append `item_to_add` to a list directly under
                `column`.
        """
        sub_key = None
        if (
            single_agent_episode is not None
            and single_agent_episode.agent_id is not None
        ):
            sub_key = (
                single_agent_episode.multi_agent_episode_id,
                single_agent_episode.agent_id,
                single_agent_episode.module_id,
            )
        elif single_agent_episode is not None:
            sub_key = (single_agent_episode.id_,)

        if column not in batch:
            batch[column] = [] if sub_key is None else {sub_key: []}
        if sub_key is not None:
            if sub_key not in batch[column]:
                batch[column][sub_key] = []
            batch[column][sub_key].append(item_to_add)
        else:
            batch[column].append(item_to_add)

    @staticmethod
    def add_n_batch_items(
        batch: Dict[str, Any],
        column: str,
        items_to_add: Any,
        num_items: int,
        single_agent_episode: Optional[SingleAgentEpisode] = None,
    ) -> None:
        """Adds a list of items (or batched item) under `column` to the given `batch`.

        If items_to_add is not a list, but an already batched struct (of np.ndarray
        leafs), will unbatch first into a list of individual batch items and add
        each individually.

        If `single_agent_episode` is provided and it contains agent ID and module ID
        information, will store the individual items in a list under a
        `([agent_id],[module_id])` key within `column`. In all other cases, will store
        the individual items in a list directly under `column`.

        .. testcode::

            import numpy as np

            from ray.rllib.connectors.connector_v2 import ConnectorV2
            from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
            from ray.rllib.env.single_agent_episode import SingleAgentEpisode
            from ray.rllib.utils.test_utils import check

            # Simple case (no episodes provided) -> Store data in a list directly under
            # `column`:
            batch = {}
            ConnectorV2.add_n_batch_items(
                batch,
                "test_col",
                # List of (complex) structs.
                [{"a": np.array(3), "b": 4}, {"a": np.array(5), "b": 6}],
                num_items=2,
            )
            check(
                batch["test_col"],
                [{"a": np.array(3), "b": 4}, {"a": np.array(5), "b": 6}],
            )
            # In a new column (test_col_2), store some already batched items.
            # This way, you may avoid having to disassemble an already batched item
            # (e.g. a numpy array of shape (10, 2)) into its individual items (e.g.
            # split the array into a list of len=10) and then adding these individually.
            # The performance gains may be quite large when providing already batched
            # items (such as numpy arrays with a batch dim):
            ConnectorV2.add_n_batch_items(
                batch,
                "test_col_2",
                # One (complex) already batched struct.
                {"a": np.array([3, 5]), "b": np.array([4, 6])},
                num_items=2,
            )
            # Add more already batched items (this time with a different batch size)
            ConnectorV2.add_n_batch_items(
                batch,
                "test_col_2",
                {"a": np.array([7, 7, 7]), "b": np.array([8, 8, 8])},
                num_items=3,  # <- in this case, this must be the batch size
            )
            check(
                batch["test_col_2"],
                [
                    {"a": np.array([3, 5]), "b": np.array([4, 6])},
                    {"a": np.array([7, 7, 7]), "b": np.array([8, 8, 8])},
                ],
            )

            # Single-agent case (SingleAgentEpisode provided) -> Store data in a list
            # under the keys: `column` -> `(eps_id,)`:
            batch = {}
            episode = SingleAgentEpisode(
                id_="SA-EPS0",
                observations=[0, 1, 2, 3],
                actions=[1, 2, 3],
                rewards=[1.0, 2.0, 3.0],
            )
            ConnectorV2.add_n_batch_items(
                batch=batch,
                column="test_col",
                items_to_add=[5, 6, 7],
                num_items=3,
                single_agent_episode=episode,
            )
            check(batch, {
                "test_col": {("SA-EPS0",): [5, 6, 7]},
            })

            # Multi-agent case (SingleAgentEpisode provided that has `agent_id` and
            # `module_id` information) -> Store data in a list under the keys:
            # `column` -> `([eps_id], [agent_id], [module_id])`:
            batch = {}
            ma_episode = MultiAgentEpisode(
                id_="MA-EPS1",
                observations=[
                    {"ag0": 0, "ag1": 1}, {"ag0": 2, "ag1": 4}
                ],
                actions=[{"ag0": 0, "ag1": 1}],
                rewards=[{"ag0": -0.1, "ag1": -0.2}],
                # ag0 maps to mod0, ag1 maps to mod1, etc..
                agent_to_module_mapping_fn=lambda aid, eps: f"mod{aid[2:]}",
            )
            ConnectorV2.add_batch_item(
                batch,
                "test_col",
                item_to_add=5,
                single_agent_episode=ma_episode.agent_episodes["ag0"],
            )
            ConnectorV2.add_batch_item(
                batch,
                "test_col",
                item_to_add=6,
                single_agent_episode=ma_episode.agent_episodes["ag0"],
            )
            ConnectorV2.add_batch_item(
                batch,
                "test_col_2",
                item_to_add=10,
                single_agent_episode=ma_episode.agent_episodes["ag1"],
            )
            check(
                batch,
                {
                    "test_col": {("MA-EPS1", "ag0", "mod0"): [5, 6]},
                    "test_col_2": {("MA-EPS1", "ag1", "mod1"): [10]},
                },
            )

        Args:
            batch: The batch to store n `items_to_add` in.
            column: The column name (str) within the `batch` to store `item_to_add`
                under.
            items_to_add: The list of data items to store in the batch OR an already
                batched (possibly nested) struct, which will first be split up into
                a list of individual items, then these individual items will be added
                to the given `column` in the batch.
            num_items: The number of items in `items_to_add`. This arg is mostly for
                asserting the correct usage of this method by checking, whether the
                given data in `items_to_add` really has the right amount of individual
                items.
            single_agent_episode: An optional SingleAgentEpisode. If provided and its
                agent_id and module_id properties are not None, will create a further
                sub dictionary under `column`, mapping from `([agent_id],[module_id])`
                (str) to a list of data items. Otherwise, will store `item_to_add`
                in a list directly under `column`.
        """
        # Process n list items by calling `add_batch_item` on each of them individually.
        if isinstance(items_to_add, list):
            if len(items_to_add) != num_items:
                raise ValueError(
                    f"Mismatch breteen `num_items` ({num_items}) and the length "
                    f"of the provided list ({len(items_to_add)}) in "
                    f"{ConnectorV2.__name__}.add_n_batch_items()!"
                )
            for item in items_to_add:
                ConnectorV2.add_batch_item(
                    batch=batch,
                    column=column,
                    item_to_add=item,
                    single_agent_episode=single_agent_episode,
                )
            return

        # Process a batched (possibly complex) struct.
        # We could just unbatch the item (split it into a list) and then add each
        # individual item to our `batch`. However, this comes with a heavy performance
        # penalty. Instead, we tag the thus added array(s) here as "_has_batch_dim=True"
        # and then know that when batching the entire list under the respective
        # (eps_id, agent_id, module_id)-tuple key, we need to concatenate, not stack
        # the items in there.
        def _tag(s):
            return BatchedNdArray(s)

        ConnectorV2.add_batch_item(
            batch=batch,
            column=column,
            # Convert given input into BatchedNdArray(s) such that the `batch` utility
            # knows that it'll have to concat, not stack.
            item_to_add=tree.map_structure(_tag, items_to_add),
            single_agent_episode=single_agent_episode,
        )

    @staticmethod
    def foreach_batch_item_change_in_place(
        batch: Dict[str, Any],
        column: Union[str, List[str], Tuple[str]],
        func: Callable[
            [Any, Optional[int], Optional[AgentID], Optional[ModuleID]], Any
        ],
    ) -> None:
        """Runs the provided `func` on all items under one or more columns in the batch.

        Use this method to conveniently loop through all items in a batch
        and transform them in place.

        `func` takes the following as arguments:
        - The item itself. If column is a list of column names, this argument is a tuple
        of items.
        - The EpisodeID. This value might be None.
        - The AgentID. This value might be None in the single-agent case.
        - The ModuleID. This value might be None in the single-agent case.

        The return value(s) of `func` are used to directly override the values in the
        given `batch`.

        Args:
            batch: The batch to process in-place.
            column: A single column name (str) or a list thereof. If a list is provided,
                the first argument to `func` is a tuple of items. If a single
                str is provided, the first argument to `func` is an individual
                item.
            func: The function to call on each item or tuple of item(s).

        .. testcode::

            from ray.rllib.connectors.connector_v2 import ConnectorV2
            from ray.rllib.utils.test_utils import check

            # Simple case: Batch items are in lists directly under their column names.
            batch = {
                "col1": [0, 1, 2, 3],
                "col2": [0, -1, -2, -3],
            }
            # Increase all ints by 1.
            ConnectorV2.foreach_batch_item_change_in_place(
                batch=batch,
                column="col1",
                func=lambda item, *args: item + 1,
            )
            check(batch["col1"], [1, 2, 3, 4])

            # Further increase all ints by 1 in col1 and flip sign in col2.
            ConnectorV2.foreach_batch_item_change_in_place(
                batch=batch,
                column=["col1", "col2"],
                func=(lambda items, *args: (items[0] + 1, -items[1])),
            )
            check(batch["col1"], [2, 3, 4, 5])
            check(batch["col2"], [0, 1, 2, 3])

            # Single-agent case: Batch items are in lists under (eps_id,)-keys in a dict
            # under their column names.
            batch = {
                "col1": {
                    ("eps1",): [0, 1, 2, 3],
                    ("eps2",): [400, 500, 600],
                },
            }
            # Increase all ints of eps1 by 1 and divide all ints of eps2 by 100.
            ConnectorV2.foreach_batch_item_change_in_place(
                batch=batch,
                column="col1",
                func=lambda item, eps_id, *args: (
                    item + 1 if eps_id == "eps1" else item / 100
                ),
            )
            check(batch["col1"], {
                ("eps1",): [1, 2, 3, 4],
                ("eps2",): [4, 5, 6],
            })

            # Multi-agent case: Batch items are in lists under
            # (eps_id, agent_id, module_id)-keys in a dict
            # under their column names.
            batch = {
                "col1": {
                    ("eps1", "ag1", "mod1"): [1, 2, 3, 4],
                    ("eps2", "ag1", "mod2"): [400, 500, 600],
                    ("eps2", "ag2", "mod3"): [-1, -2, -3, -4, -5],
                },
            }
            # Decrease all ints of "eps1" by 1, divide all ints of "mod2" by 100, and
            # flip sign of all ints of "ag2".
            ConnectorV2.foreach_batch_item_change_in_place(
                batch=batch,
                column="col1",
                func=lambda item, eps_id, ag_id, mod_id: (
                    item - 1
                    if eps_id == "eps1"
                    else item / 100
                    if mod_id == "mod2"
                    else -item
                ),
            )
            check(batch["col1"], {
                ("eps1", "ag1", "mod1"): [0, 1, 2, 3],
                ("eps2", "ag1", "mod2"): [4, 5, 6],
                ("eps2", "ag2", "mod3"): [1, 2, 3, 4, 5],
            })
        """
        data_to_process = [batch.get(c) for c in force_list(column)]
        single_col = isinstance(column, str)
        if any(d is None for d in data_to_process):
            raise ValueError(
                f"Invalid column name(s) ({column})! One or more not found in "
                f"given batch. Found columns {list(batch.keys())}."
            )

        # Simple case: Data items are stored in a list directly under the column
        # name(s).
        if isinstance(data_to_process[0], list):
            for list_pos, data_tuple in enumerate(zip(*data_to_process)):
                results = func(
                    data_tuple[0] if single_col else data_tuple,
                    None,  # episode_id
                    None,  # agent_id
                    None,  # module_id
                )
                # Tuple'ize results if single_col.
                results = (results,) if single_col else results
                for col_slot, result in enumerate(force_list(results)):
                    data_to_process[col_slot][list_pos] = result
        # Single-agent/multi-agent cases.
        else:
            for key, d0_list in data_to_process[0].items():
                # Multi-agent case: There is a dict mapping from a
                # (eps id, AgentID, ModuleID)-tuples to lists of individual data items.
                if len(key) == 3:
                    eps_id, agent_id, module_id = key
                # Single-agent case: There is a dict mapping from a (eps_id,)-tuple
                # to lists of individual data items.
                # AgentID and ModuleID are both None.
                else:
                    eps_id = key[0]
                    agent_id = module_id = None
                other_lists = [d[key] for d in data_to_process[1:]]
                for list_pos, data_tuple in enumerate(zip(d0_list, *other_lists)):
                    results = func(
                        data_tuple[0] if single_col else data_tuple,
                        eps_id,
                        agent_id,
                        module_id,
                    )
                    # Tuple'ize results if single_col.
                    results = (results,) if single_col else results
                    for col_slot, result in enumerate(results):
                        data_to_process[col_slot][key][list_pos] = result

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
        return

    def reset_state(self) -> None:
        """Resets the state of this ConnectorV2 to some initial value.

        Note that this may NOT be the exact state that this ConnectorV2 was originally
        constructed with.
        """
        return

    def merge_states(self, states: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Computes a resulting state given self's state and a list of other states.

        Algorithms should use this method for merging states between connectors
        running on parallel EnvRunner workers. For example, to synchronize the connector
        states of n remote workers and a local worker, one could:
        - Gather all remote worker connector states in a list.
        - Call `self.merge_states()` on the local worker passing it the states list.
        - Broadcast the resulting local worker's connector state back to all remote
        workers. After this, all workers (including the local one) hold a
        merged/synchronized new connecto state.

        Args:
            states: The list of n other ConnectorV2 states to merge with self's state
                into a single resulting state.

        Returns:
            The resulting state dict.
        """
        return {}

    @property
    def observation_space(self):
        """Getter for our (output) observation space.

        Logic: Use user provided space (if set via `observation_space` setter)
        otherwise, use the same as the input space, assuming this connector piece
        does not alter the space.
        """
        return self._observation_space

    @property
    def action_space(self):
        """Getter for our (output) action space.

        Logic: Use user provided space (if set via `action_space` setter)
        otherwise, use the same as the input space, assuming this connector piece
        does not alter the space.
        """
        return self._action_space

    @property
    def input_observation_space(self):
        return self._input_observation_space

    @input_observation_space.setter
    def input_observation_space(self, value):
        self._input_observation_space = value
        if value is not None:
            self._observation_space = (
                self.recompute_observation_space_from_input_spaces()
            )

    @property
    def input_action_space(self):
        return self._input_action_space

    @input_action_space.setter
    def input_action_space(self, value):
        self._input_action_space = value
        if value is not None:
            self._action_space = self.recompute_action_space_from_input_spaces()

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__
