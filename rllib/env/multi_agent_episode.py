import numpy as np
import uuid

from typing import Any, Dict, List, Optional, Union

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.typing import MultiAgentDict


# TODO (simon): Include cases in which the number of agents in an
# episode are shrinking or growing during the episode itself.
class MultiAgentEpisode:
    """Stores multi-agent episode data.

    The central attribute of the class is the timestep mapping
    `global_t_to_local_t` that maps the global (environment)
    timestep to the local (agent) timesteps.

    The `MultiAgentEpisode` is based on the `SingleAgentEpisode`s
    for each agent, stored in `MultiAgentEpisode.agent_episodes`.
    """

    def __init__(
        self,
        id_: Optional[str] = None,
        agent_ids: List[str] = None,
        agent_episode_ids: Optional[Dict[str, str]] = None,
        *,
        observations: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        states: Optional[List[MultiAgentDict]] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        t_started: int = 0,
        is_terminated: Optional[bool] = False,
        is_truncated: Optional[bool] = False,
        render_images: Optional[List[np.ndarray]] = None,
        extra_model_outputs: Optional[List[MultiAgentDict]] = None,
        # TODO (simon): Also allow to receive `extra_model_outputs`.
        # TODO (simon): Validate terminated/truncated for env/agents.
    ) -> "MultiAgentEpisode":
        """Initializes a `MultiAgentEpisode`.

        Args:
            id_: Optional. Either a string to identify an episode or None.
                If None, a hexadecimal id is created. In case of providing
                a string, make sure that it is unique, as episodes get
                concatenated via this string.
            agent_ids: Obligatory. A list of strings containing the agent ids.
                These have to be provided at initialization.
            agent_episode_ids: Optional. Either a dictionary mapping agent ids
                corresponding `SingleAgentEpisode` or None. If None, each
                `SingleAgentEpisode` in `MultiAgentEpisode.agent_episodes`
                will generate a hexadecimal code. If a dictionary is provided
                make sure that ids are unique as agents'  `SingleAgentEpisode`s
                get concatenated or recreated by it.
            observations: A dictionary mapping from agent ids to observations.
                Can be None. If provided, it should be provided together with
                all other episode data (actions, rewards, etc.)
            actions: A dictionary mapping from agent ids to corresponding actions.
                Can be None. If provided, it should be provided together with
                all other episode data (observations, rewards, etc.).
            rewards: A dictionary mapping from agent ids to corresponding rewards.
                Can be None. If provided, it should be provided together with
                all other episode data (observations, rewards, etc.).
            infos: A dictionary mapping from agent ids to corresponding infos.
                Can be None. If provided, it should be provided together with
                all other episode data (observations, rewards, etc.).
            states: A dictionary mapping from agent ids to their corresponding
                modules' hidden states. These will be stored into the
                `SingleAgentEpisode`s in `MultiAgentEpisode.agent_episodes`.
                Can be None.
            t_started: Optional. An unsigned int that defines the starting point
                of the episode. This is only different from zero, if an ongoing
                episode is created.
            is_terminazted: Optional. A boolean defining, if an environment has
                terminated. The default is `False`, i.e. the episode is ongoing.
            is_truncated: Optional. A boolean, defining, if an environment is
                truncated. The default is `False`, i.e. the episode is ongoing.
            render_images: Optional. A list of RGB uint8 images from rendering
                the environment.
            extra_model_outputs: Optional. A dictionary mapping agent ids to their
                corresponding extra model outputs. Each of the latter is a list of
                dictionaries containing specific model outputs for the algorithm
                used (e.g. `vf_preds` and `action_logp` for PPO) from a rollout.
                If data is provided it should be complete (i.e. observations,
                actions, rewards, is_terminated, is_truncated, and all necessary
                `extra_model_outputs`).
        """

        self.id_: str = id_ or uuid.uuid4().hex

        # Agent ids must be provided if data is provided. The Episode cannot
        # know how many agents are in the environment. Also the number of agents
        # can grwo or shrink.
        self._agent_ids: Union[List[str], List[object]] = (
            [] if agent_ids is None else agent_ids
        )

        # The global last timestep of the episode and the timesteps when this chunk
        # started.
        self.t = self.t_started = (
            t_started if t_started is not None else max(len(observations) - 1, 0)
        )
        # Keeps track of the correspondence between agent steps and environment steps.
        # This is a mapping from agents to `IndexMapping`. The latter keeps
        # track of the global timesteps at which an agent stepped.
        self.global_t_to_local_t: Dict[str, List[int]] = self._generate_ts_mapping(
            observations
        )

        # Note that all attributes will be recorded along the global timestep
        # in an multi-agent environment. `SingleAgentEpisodes`
        self.agent_episodes: MultiAgentDict = {
            agent_id: self._generate_single_agent_episode(
                agent_id,
                agent_episode_ids,
                observations,
                actions,
                rewards,
                infos,
                states,
                extra_model_outputs,
            )
            for agent_id in self._agent_ids
        }

        # obs[-1] is the final observation in the episode.
        self.is_terminated: bool = is_terminated
        # obs[-1] is the last obs in a truncated-by-the-env episode (there will no more
        # observations in following chunks for this episode).
        self.is_truncated: bool = is_truncated
        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images: Union[List[np.ndarray], List[object]] = (
            [] if render_images is None else render_images
        )

    def concat_episode(self, episode_chunk: "MultiAgentEpisode") -> None:
        """Adds the given `episode_chunk` to the right side of self.

        For concatenating episodes the following rules hold:
            - IDs are identical.
            - timesteps match (`t` of `self` matches `t_started` of `episode_chunk`).

        Args:
            episode_chunk: `MultiAgentEpsiode` instance that should be concatenated
                to `self`.
        """
        assert episode_chunk.id_ == self.id_
        assert not self.is_done
        # Make sure the timesteps match.
        assert self.t == episode_chunk.t_started

        # TODO (simon): Write `validate()` method.

        # Make sure, end matches `episode_chunk`'s beginning for all agents.
        observations: MultiAgentDict = self.get_observations()
        for agent_id, agent_obs in episode_chunk.get_observations(indices=0):
            # Make sure that the same agents stepped at both timesteps.
            assert agent_id in observations
            assert observations[agent_id] == agent_obs
        # Pop out the end for the agents that stepped.
        for agent_id in observations:
            self.agent_episodes[agent_id].observations.pop()

        # Call the `SingleAgentEpisode`'s `concat_episode()` method for all agents.
        for agent_id, agent_eps in self.agent_episodes:
            agent_eps[agent_id].concat_episode(episode_chunk.agent_episodes[agent_id])
            # Update our timestep mapping.
            # TODO (simon): Check, if we have to cut off here as well.
            self.global_t_to_local_t[agent_id][
                :-1
            ] += episode_chunk.global_t_to_local_t[agent_id]

        self.t = episode_chunk.t
        if episode_chunk.is_terminated:
            self.is_terminated = True
        if episode_chunk.is_truncated:
            self.is_truncated = True

        # Validate
        # TODO (simon): Write validate function.
        # self.validate()

    # TODO (simon): Maybe adding agent axis. We might need only some agent observations.
    # Then also add possibility to get __all__ obs (or None)
    # Write many test cases (numbered obs).
    def get_observations(
        self, indices: Union[int, List[int]] = -1, global_ts: bool = True
    ) -> MultiAgentDict:
        """Gets observations for all agents that stepped in the last timesteps.

        Note that observations are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the observations
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to observations (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, observations are returned (i.e. not all agent ids are
            necessarily in the keys).
        """

        return self._getattr_by_index("observations", indices, global_ts)

    def get_infos(
        self, indices: Union[int, List[int]] = -1, global_ts: bool = True
    ) -> MultiAgentDict:
        """Gets infos for all agents that stepped in the last timesteps.

        Note that infos are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the infos
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to infos (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, infos are returned (i.e. not all agent ids are
            necessarily in the keys).
        """
        return self._getattr_by_index("infos", indices, global_ts)

    def get_actions(
        self, indices: Union[int, List[int]] = -1, global_ts: bool = True
    ) -> MultiAgentDict:
        """Gets actions for all agents that stepped in the last timesteps.

        Note that actions are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the actions
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to actions (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, actions are returned (i.e. not all agent ids are
            necessarily in the keys).
        """

        return self._getattr_by_index("actions", indices, global_ts)

    def get_rewards(
        self, indices: Union[int, List[int]] = -1, global_ts: bool = True
    ) -> MultiAgentDict:
        """Gets rewards for all agents that stepped in the last timesteps.

        Note that rewards are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the rewards
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to rewards (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, rewards are returned (i.e. not all agent ids are
            necessarily in the keys).
        """
        return self._getattr_by_index("rewards", indices, global_ts)

    def get_extra_model_outputs(
        self, indices: Union[int, List[int]] = -1, global_ts: bool = True
    ) -> MultiAgentDict:
        """Gets extra model outputs for all agents that stepped in the last timesteps.

        Note that extra model outputs are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the extra model outputs.
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to extra model outputs (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, extra model outputs are returned (i.e. not all agent ids are
            necessarily in the keys).
        """
        return self._getattr_by_index("extra_model_outputs", indices, global_ts)

    def add_initial_observation(
        self,
        *,
        initial_observation: MultiAgentDict,
        initial_info: Optional[MultiAgentDict] = None,
        initial_state: Optional[MultiAgentDict] = None,
        initial_render_image: Optional[np.ndarray] = None,
    ) -> None:
        """Stores initial observation.

        Args:
            initial_observation: Obligatory. A dictionary, mapping agent ids
                to initial observations. Note that not all agents must have
                an initial observation.
            initial_info: Optional. A dictionary, mapping agent ids to initial
                infos. Note that not all agents must have an initial info.
            initial_state: Optional. A dictionary, mapping agent ids to the
                initial hidden states of their corresponding model (`RLModule`).
                Note, this is only available, if the models are stateful. Note
                also that not all agents must have an initial state at `t=0`.
            initial_render_image: An RGB uint8 image from rendering the
                environment.
        """
        assert not self.is_done
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0

        # TODO (simon): After clearing with sven for initialization of timesteps
        # this might be removed.
        if len(self.global_t_to_local_t) == 0:
            self.global_t_to_local_t = {agent_id: [] for agent_id in self._agent_ids}

        # Note that we store the render images into the `MultiAgentEpisode`
        # instead into each `SingleAgentEpisode`.
        if initial_render_image is not None:
            self.render_images.append(initial_render_image)

        # Note, all agents will have an initial observation.
        for agent_id in initial_observation.keys():
            # Add initial timestep for each agent to the timestep mapping.
            self.global_t_to_local_t[agent_id].append(self.t)

            # Add initial observations to the agent's episode.
            self.agent_episodes[agent_id].add_initial_observation(
                # Note, initial observation has to be provided.
                initial_observation=initial_observation[agent_id],
                initial_info=None if initial_info is None else initial_info[agent_id],
                initial_state=None
                if initial_state is None
                else initial_state[agent_id],
            )

    def add_timestep(
        self,
        observation: MultiAgentDict,
        action: MultiAgentDict,
        reward: MultiAgentDict,
        *,
        info: Optional[MultiAgentDict] = None,
        state: Optional[MultiAgentDict] = None,
        is_terminated: Optional[bool] = None,
        is_truncated: Optional[bool] = None,
        render_image: Optional[np.ndarray] = None,
        extra_model_output: Optional[MultiAgentDict] = None,
    ) -> None:
        """Adds a timestep to the episode.

        Args:
            observation: Mandatory. A dictionary, mapping agent ids to their
                corresponding observations. Note that not all agents must have stepped
                a this timestep.
            action: Mandatory. A dictionary, mapping agent ids to their
                corresponding actions. Note that not all agents must have stepped
                a this timestep.
            reward: Mandatory. A dictionary, mapping agent ids to their
                corresponding observations. Note that not all agents must have stepped
                a this timestep.
            info: Optional. A dictionary, mapping agent ids to their
                corresponding info. Note that not all agents must have stepped
                a this timestep.
            state: Optional. A dictionary, mapping agent ids to their
                corresponding hidden model state. Note, this is only available for a
                stateful model. Also note that not all agents must have stepped a this
                timestep.
            is_terminated: A boolean indicating, if the environment has been
                terminated.
            is_truncated: A boolean indicating, if the environment has been
                truncated.
            render_image: Optional. An RGB uint8 image from rendering the environment.
            extra_model_output: Optional. A dictionary, mapping agent ids to their
                corresponding specific model outputs (also in a dictionary; e.g.
                `vf_preds` for PPO).
        """
        # Cannot add data to an already done episode.
        assert not self.is_done

        # Environment step.
        self.t += 1

        # TODO (sven, simon): Wilol there still be an `__all__` that is
        # terminated or truncated?
        self.is_terminated = (
            False if is_terminated is None else is_terminated["__all__"]
        )
        self.is_truncated = False if is_truncated is None else is_truncated["__all__"]

        # Add data to agent episodes.
        for agent_id in observation.keys():
            # If the agent stepped we need to keep track in the timestep mapping.
            self.global_t_to_local_t[agent_id].append(self.t)

            # Note that we store the render images into the `MultiAgentEpisode`
            # instead of storing them into each `SingleAgentEpisode`.
            if render_image is not None:
                self.render_images.append(render_image)

            self.agent_episodes[agent_id].add_timestep(
                observation[agent_id],
                action[agent_id],
                reward[agent_id],
                info=None if info is None else info[agent_id],
                state=None if state is None else state[agent_id],
                is_terminated=None
                if is_terminated is None
                else is_terminated[agent_id],
                is_truncated=None if is_truncated is None else is_truncated[agent_id],
                render_image=None if render_image is None else render_image[agent_id],
                extra_model_output=None
                if extra_model_output is None
                else extra_model_output[agent_id],
            )

    @property
    def is_done(self):
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_timestep()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.create_successor()`.

        Note that in a multi-agent environment this does not necessarily
        correspond to single agents having terminated or being truncated.

        `self.is_terminated` should be `True`, if all agents are terminated and
        `self.is_truncated` should be `True`, if all agents are truncated. If
        only one or more (but not all!) agents are `terminated/truncated the
        `MultiAgentEpisode.is_terminated/is_truncated` should be `False`. This
        information about single agent's terminated/truncated states can always
        be retrieved from the `SingleAgentEpisode`s inside the 'MultiAgentEpisode`
        one.

        If all agents are either terminated or truncated, but in a mixed fashion,
        i.e. some are terminated and others are truncated: This is currently
        undefined and could potentially be a problem (if a user really implemented
        such a multi-agent env that behaves this way).

        Returns:
            Boolean defining if an episode has either terminated or truncated.
        """
        return self.is_terminated or self.is_truncated

    def create_successor(self) -> "MultiAgentEpisode":
        """Restarts an ongoing episode from its last observation.

        Note, this method is used so far specifically for the case of
        `batch_mode="truncated_episodes"` to ensure that episodes are
        immutable inside the `EnvRunner` when truncated and passed over
        to postprocessing.

        The newly created `MultiAgentEpisode` contains the same id, and
        starts at the timestep where it's predecessor stopped in the last
        rollout. Last observations, infos, rewards, etc. are carried over
        from the predecessor. This also helps to not carry stale data that
        had been collected in the last rollout when rolling out the new
        policy in the next iteration (rollout).

        Returns: A MultiAgentEpisode starting at the timepoint where
            its predecessor stopped.
        """
        assert not self.is_done

        # Get the last multi-agent observation and info.
        observations = self.get_observations()
        infos = self.get_infos()
        # It is more safe to use here a list of episode ids instead of
        # calling `create_successor()` as we need as the single source
        # of truth always the `global_t_to_local_t` timestep mapping.
        return MultiAgentEpisode(
            id=self.id_,
            agent_episode_ids={
                agent_id: agent_eps.id_ for agent_id, agent_eps in self.agent_episodes
            },
            observations=observations,
            infos=infos,
            t_started=self.t,
        )

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of a multi-agent episode.

        Note that from an episode's state the episode itself can
        be recreated.

        Returns: A dicitonary containing pickable data fro a
            `MultiAgentEpisode`.
        """
        return list(
            {
                "id_": self.id_,
                "agent_ids": self._agent_ids,
                "global_t_to_local_t": self.global_t_to_local_t,
                "agent_episodes": list(
                    {
                        agent_id: agent_eps.get_state()
                        for agent_id, agent_eps in self.agent_episodes.items()
                    }.items()
                ),
                "t_started": self.t_started,
                "t": self.t,
                "is_terminated": self.is_terminated,
                "is_truncated": self.is_truncated,
            }.items()
        )

    @staticmethod
    def from_state(state) -> None:
        """Creates a multi-agent episode from a state dictionary.

        See `MultiAgentEpisode.get_state()` for creating a state for
        a `MultiAgentEpisode` pickable state. For recreating a
        `MultiAgentEpisode` from a state, this state has to be complete,
        i.e. all data must have been stored in the state.
        """
        eps = MultiAgentEpisode(id=state[0][1])
        eps._agent_ids = state[1][1]
        eps.global_t_to_local_t = state[2][1]
        eps.agent_episodes = {
            agent_id: SingleAgentEpisode.from_state(agent_state)
            for agent_id, agent_state in state[3][1]
        }
        eps.t_started = state[3][1]
        eps.t = state[4][1]
        eps.is_terminated = state[5][1]
        eps.is_trcunated = state[6][1]
        return eps

    def to_sample_batch(self) -> MultiAgentBatch:
        """Converts a `MultiAgentEpisode` into a `MultiAgentBatch`.

        Each `SingleAgentEpisode` instances in
        `MultiAgentEpisode.agent_epiosdes` will be converted into
        a `SampleBatch` and the environment timestep will be passed
        towards the `MultiAgentBatch`'s `count`.

        Returns: A `MultiAgentBatch` instance.
        """
        # TODO (simon): Check, if timesteps should be converted into global
        # timesteps instead of agent steps.
        return MultiAgentBatch(
            policy_batches={
                agent_id: agent_eps.to_sample_batch()
                for agent_id, agent_eps in self.agent_episodes.items()
            },
            env_steps=self.t,
        )

    def get_return(self) -> float:
        """Get the all-agent return.

        Returns: A float. The aggregate return from all agents.
        """
        return sum(
            [agent_eps.get_return() for agent_eps in self.agent_episodes.values()]
        )

    def _generate_ts_mapping(
        self, observations: List[MultiAgentDict]
    ) -> MultiAgentDict:
        """Generates a timestep mapping to local agent timesteps.

        This helps us to keep track of which agent stepped at
        which global (environment) timestep.
        Note that the local (agent) timestep is given by the index
        of the list for each agent.

        Args:
            observations: A list of observations.Each observations maps agent
                ids to their corresponding observation.

        Returns: A dictionary mapping agents to time index lists. The latter
            contain the global (environment) timesteps at which the agent
            stepped (was ready).
        """
        # Only if agent ids have been provided we can build the timestep mapping.
        if len(self._agent_ids) > 0:
            global_t_to_local_t = {agent: _IndexMapping() for agent in self._agent_ids}

            # We need the observations to create the timestep mapping.
            if len(observations) > 0:
                for t, agent_map in enumerate(observations):
                    for agent_id in agent_map:
                        # If agent stepped add the timestep to the timestep mapping.
                        global_t_to_local_t[agent_id].append(t)
            # Otherwise, set to an empoty dict (when creating an empty episode).
            else:
                global_t_to_local_t = {}
        # Otherwise, set to an empoty dict (when creating an empty episode).
        else:
            # TODO (sven, simon): Shall we return an empty dict or an agent dict with
            # empty lists?
            global_t_to_local_t = {}
        # Return the index mapping.
        return global_t_to_local_t

    # TODO (simon): Add infos.
    def _generate_single_agent_episode(
        self,
        agent_id: str,
        agent_episode_ids: Optional[Dict[str, str]] = None,
        observations: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        states: Optional[MultiAgentDict] = None,
        extra_model_outputs: Optional[MultiAgentDict] = None,
    ) -> SingleAgentEpisode:
        """Generates a `SingleAgentEpisode` from multi-agent data.

        Note, if no data is provided an empty `SingleAgentEpiosde`
        will be returned that starts at `SIngleAgentEpisode.t_started=0`.

        Args:
            agent_id: String, idnetifying the agent for which the data should
                be extracted.
            agent_episode_ids: Optional. A dictionary mapping agents to
                corresponding episode ids. If `None` the `SingleAgentEpisode`
                creates a hexadecimal code.
            observations: Optional. A list of dictionaries, each mapping
                from agent ids to observations. When data is provided
                it should be complete, i.e. observations, actions, rewards,
                etc. should be provided.
            actions: Optional. A list of dictionaries, each mapping
                from agent ids to actions. When data is provided
                it should be complete, i.e. observations, actions, rewards,
                etc. should be provided.
            rewards: Optional. A list of dictionaries, each mapping
                from agent ids to rewards. When data is provided
                it should be complete, i.e. observations, actions, rewards,
                etc. should be provided.
            infos: Optional. A list of dictionaries, each mapping
                from agent ids to infos. When data is provided
                it should be complete, i.e. observations, actions, rewards,
                etc. should be provided.
            states: Optional. A dicitionary mapping each agent to it's
                module's hidden model state (if the model is stateful).
            extra_model_outputs: Optional. A list of agent mappings for every
                timestep. Each of these dictionaries maps an agent to its
                corresponding `extra_model_outputs`, which a re specific model
                outputs needed by the algorithm used (e.g. `vf_preds` and
                `action_logp` for PPO). f data is provided it should be complete
                (i.e. observations, actions, rewards, is_terminated, is_truncated,
                and all necessary `extra_model_outputs`).

        Returns: An instance of `SingleAgentEpisode` containing the agent's
            extracted episode data.
        """

        # If an episode id for an agent episode was provided assign it.
        episode_id = None if agent_episode_ids is None else agent_episode_ids[agent_id]
        # We need the timestep mapping to create single agent's episode.
        if len(self.global_t_to_local_t) > 0:
            # Set to None if not provided.
            agent_observations = (
                None
                if observations is None
                else self._get_single_agent_data(agent_id, observations)
            )

            # Note, the timestep mapping is deduced from observations and starts one
            # timestep earlier. Therefore all other data is missing the last index.
            agent_actions = (
                None
                if actions is None
                else self._get_single_agent_data(
                    agent_id, actions, start_index=1, shift=-1
                )
            )
            agent_rewards = (
                None
                if rewards is None
                else self._get_single_agent_data(
                    agent_id, rewards, start_index=1, shift=-1
                )
            )
            # Like observations, infos start at timestep `t=0`, so we do not need to
            # shift.
            agent_infos = (
                None
                if infos is None
                else self._get_single_agent_data(agent_id, infos, start_index=1)
            )
            agent_states = (
                None
                if states is None
                else self._get_single_agent_data(
                    agent_id, states, start_index=1, shift=-1
                )
            )
            agent_extra_model_outputs = (
                None
                if extra_model_outputs is None
                else self._get_single_agent_data(
                    agent_id, extra_model_outputs, start_index=1, shift=-1
                )
            )

            return SingleAgentEpisode(
                id_=episode_id,
                observations=agent_observations,
                actions=agent_actions,
                rewards=agent_rewards,
                infos=agent_infos,
                states=agent_states,
                extra_model_outputs=agent_extra_model_outputs,
            )
        # Otherwise return empty ' SingleAgentEpisosde'.
        else:
            return SingleAgentEpisode(id_=episode_id)

    def _getattr_by_index(
        self,
        attr: str = "observations",
        indices: Union[int, List[int]] = -1,
        global_ts: bool = True,
    ) -> MultiAgentDict:
        # First for global_ts = True:
        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [self.t + (idx if idx < 0 else idx) for idx in indices]
            else:
                indices = [self.t + indices] if indices < 0 else [indices]

            return {
                agent_id: list(
                    map(
                        getattr(agent_eps, attr).__getitem__,
                        self.global_t_to_local_t[agent_id].find_indices(indices),
                    )
                )
                for agent_id, agent_eps in self.agent_episodes.items()
                # Only include agent data for agents that stepped.
                if len(self.global_t_to_local_t[agent_id].find_indices(indices)) > 0
            }
        # Otherwise just look for the timesteps in the `SingleAgentEpisode`s
        # directly.
        else:
            # Check, if the indices are iterable.
            if not isinstance(indices, list):
                indices = [indices]

            return {
                agent_id: list(map(getattr(agent_eps, attr).__getitem__, indices))
                for agent_id, agent_eps in self.agent_episodes.items()
                # Only include agent data for agents that stepped so far at least once.
                # TODO (sven, simon): This will not include initial observations. Should
                # we?
                if self.agent_episodes[agent_id].t > 0
            }

    def _get_single_agent_data(
        self,
        agent_id: str,
        ma_data: List[MultiAgentDict],
        start_index: int = 0,
        end_index: Optional[int] = None,
        shift: int = 0,
    ) -> List[Any]:
        """Returns single agent data from multi-agent data.

        Args:
            agent_id: A string identifying the agent for which the
                data should be extracted.
            ma_data: A List of dictionaries, each containing multi-agent
                data, i.e. mapping from agent ids to timestep data.
            start_index: An integer defining the start point of the
                extration window. The default starts at the beginning of the
                the `ma_data` list.
            end_index: Optional. An integer defining the end point of the
                extraction window. If `None`, the extraction window will be
                until the end of the `ma_data` list.g
            shift: An integer that defines by which amount to shift the
                running index for extraction. This is for example needed
                when we extract data that started at index 1.

        Returns: A list containing single-agent data from the multi-agent
            data provided.
        """
        # Return all single agent data along the global timestep.
        return [
            singleton[agent_id]
            for singleton in list(
                map(
                    ma_data.__getitem__,
                    [
                        i + shift
                        for i in self.global_t_to_local_t[agent_id][
                            start_index:end_index
                        ]
                    ],
                )
            )
            if agent_id in singleton.keys()
        ]

    def __len__(self):
        """Returns the length of an `MultiAgentEpisode`.

        Note that the length of an episode is defined by the difference
        between its actual timestep and the starting point.

        Returns: An integer defining the length of the episode or an
            error if the episode has not yet started.
        """
        assert self.t_started < self.t, (
            "ERROR: Cannot determine length of episode that hasn't started, yet!"
            "Call `MultiAgentEpisode.add_initial_observation(initial_observation=)` "
            "first (after which `len(MultiAgentEpisode)` will be 0)."
        )
        return self.t - self.t_started


class _IndexMapping(list):
    """Provides lists with a method to find multiple elements.

    This class is used for the timestep mapping which is central to
    the multi-agent episode. For each agent the timestep mapping is
    implemented with an `IndexMapping`.

    The `IndexMapping.find_indices` method simplifies the search for
    multiple environment timesteps at which some agents have stepped.
    See for example `MultiAgentEpisode.get_observations()`.
    """

    def find_indices(self, indices_to_find: List[int]):
        """Returns global timesteps at which an agent stepped.

        The function returns for a given list of indices the ones
        that are stored in the `IndexMapping`.

        Args:
            indices_to_find: A list of indices that should be
                found in the `IndexMapping`.

        Returns:
            A list of indices at which to find the `indices_to_find`
            in `self`. This could be empty if none of the given
            indices are in `IndexMapping`.
        """
        indices = []
        for num in indices_to_find:
            if num in self:
                indices.append(self.index(num))
        return indices
