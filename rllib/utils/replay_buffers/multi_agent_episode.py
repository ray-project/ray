import uuid
from typing import Any, Dict, List, Optional, Union

from ray.rllib.utils.replay_buffers.episode_replay_buffer import SingleAgentEpisode


class IndexMapping(list):
    """Provides lists with a method to find elements."""

    def find_indices(self, indices_to_find):
        indices = []
        for num in indices_to_find:
            if num in self:
                indices.append(self.index(num))
        return indices


# TODO (sven, simon): This is still the old _Episode, i.e. without
# extra_model_outputs, etc. Change the episode here when the other
# one is merged.
class MultiAgentEpisode:
    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations=None,
        actions=None,
        rewards=None,
        states=None,
        t: int = 0,
        is_terminated: bool = False,
        is_truncated: bool = False,
        render_images=None,
        agent_ids=None
        # TODO (simon): Also allow to receive `extra_model_outputs`.
    ):
        self.id_ = id_ or uuid.uuid4().hex

        # Agent ids must be provided if data is provided. The Episode cannot
        # know how many agents are in the environment. Also the number of agents
        # can grwo or shrink.
        self.agent_ids: list = [] if agent_ids is None else agent_ids

        # The global last timestep of the episode and the timesteps when this chunk
        # started.
        self.t = self.t_started = t if observations is None else len(observations) - 1
        # Keeps track of agent steps in relation to environment steps.
        # This is a mapping from agents to `IndexMapping`. The latter keeps
        # track for each agent the global timesteps at which the agent stepped.
        self.global_t_to_local_t: Dict[str, List[int]] = self._generate_ts_mapping(
            observations
        )

        # Note that all attributes will be recorded along the global timestep
        # in an multi-agent environment. `SingleAgentEpisodes`
        self.agent_episodes = {
            agent_id: self._generate_single_agent_episode(
                agent_id, observations, actions, rewards, states
            )
            for agent_id in self.agent_ids
        }

        # obs[-1] is the final observation in the episode.
        self.is_terminated = is_terminated
        # obs[-1] is the last obs in a truncated-by-the-env episode (there will no more
        # observations in following chunks for this episode).
        self.is_truncated = is_truncated

    def get_observations(self, indices: Union[int, List[int]] = -1, global_ts=True):
        """Gets observations for all agents that stepped in the last timesteps.

        Note that observations are only returned for agents that stepped
        during the given index range.
        """

        # First for global_ts = True:
        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [self.t + idx for idx in indices]
            else:
                indices = [self.t + indices]

            return {
                agent_id: list(
                    map(
                        agent_eps.observations.__getitem__,
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
                agent_id: list(map(agent_eps.observations.__getitem__, indices))
                for agent_id, agent_eps in self.agent_episodes.items()
                # Only include agent data for agents that stepped so far at least once.
                # TODO (sven, simon): This will not include initial observations. Should
                # we?
                if self.agent_episodes[agent_id].t > 0
            }

    def get_actions(self, indices: Union[int, List[int]] = -1, global_ts=True):
        """Gets actions for all agents that stepped in the last timesteps.

        Note that actions are only returned for agents that stepped
        during the given index range.
        """

        # First for global_ts = True:
        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [self.t + idx for idx in indices]
            else:
                indices = [self.t + indices]

            return {
                agent_id: list(
                    map(
                        agent_eps.actions.__getitem__,
                        self.global_t_to_local_t[agent_id].find_indices(indices),
                    )
                )
                for agent_id, agent_eps in self.agent_episodes.items()
                # Only include agent data for agents that stepped.
                if self.global_t_to_local_t[agent_id].find_indices(indices) > 0
            }
        # Otherwise just look for the timesteps in the `SingleAgentEpisode`s
        # directly.
        else:
            # Check, if the indices are iterable.
            if not isinstance(indices, list):
                indices = [indices]

            return {
                agent_id: list(map(agent_eps.actions.__getitem__, indices))
                for agent_id, agent_eps in self.agent_episodes.items()
                # Only include agent data for agents that stepped so far at least once.
                # TODO (sven, simon): This will not include initial observations. Should
                # we?
                if self.agent_episodes[agent_id].t > 0
            }

    def get_rewards(self, indices: Union[int, List[int]] = -1, global_ts=True):
        """Gets rewards for all agents that stepped in the last timesteps.

        Note that rewards are only returned for agents that stepped
        during the given index range.
        """

        # First for global_ts = True:
        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [self.t + idx for idx in indices]
            else:
                indices = [self.t + indices]

            return {
                agent_id: list(
                    map(
                        agent_eps.rewards.__getitem__,
                        self.global_t_to_local_t[agent_id].find_indices(indices),
                    )
                )
                for agent_id, agent_eps in self.agent_episodes.items()
                # Only include agent data for agents that stepped.
                if self.global_t_to_local_t[agent_id].find_indices(indices) > 0
            }
        # Otherwise just look for the timesteps in the `SingleAgentEpisode`s
        # directly.
        else:
            # Check, if the indices are iterable.
            if not isinstance(indices, list):
                indices = [indices]

            return {
                agent_id: list(map(agent_eps.rewards.__getitem__, indices))
                for agent_id, agent_eps in self.agent_episodes.items()
                # Only include agent data for agents that stepped so far at least once.
                # TODO (sven, simon): This will not include initial observations. Should
                # we?
                if self.agent_episodes[agent_id].t > 0
            }

    def add_initial_observation(
        self,
        *,
        initial_observation,
        initial_state=None,
        initial_render_image=None,
    ):
        assert not self.is_done
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0

        # TODO (simon): After clearing with sven for initialization of timestepo
        # this might be removed.
        if len(self.global_t_to_local_t) == 0:
            self.global_t_to_local_t = {agent_id: [] for agent_id in self.agent_ids}

        # Note, all agents will have an initial observation.
        for agent_id in initial_observation.keys():
            # Add initial timestep for each agent to the timestep mapping.
            self.global_t_to_local_t[agent_id].append(self.t)

            # Add initial observations to the agent's episode.
            self.agent_episodes[agent_id].add_initial_observation(
                # Note, initial observation has to be provided.
                initial_observation=initial_observation[agent_id],
                initial_state=None
                if initial_state is None
                else initial_state[agent_id],
                initial_render_image=None
                if initial_render_image is None
                else initial_render_image[agent_id],
            )

    def add_timestep(
        self,
        observation,
        action,
        reward,
        *,
        state=None,
        is_terminated=None,
        is_truncated=None,
        render_image=None,
    ) -> None:
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
            self.agent_episodes[agent_id].add_timestep(
                observation[agent_id],
                action[agent_id],
                reward[agent_id],
                state=None if state is None else state[agent_id],
                is_terminated=None
                if is_terminated is None
                else is_terminated[agent_id],
                is_truncated=None if is_truncated is None else is_truncated[agent_id],
                render_image=None if render_image is None else render_image[agent_id],
            )

    @property
    def is_done(self):
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_timestep()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.create_successor()`.

        Note that in a multi-agent environment this does not necessarily
        correspond to single agents having terminated or being truncated.
        """
        return self.is_terminated or self.is_truncated

    def _generate_ts_mapping(self, observations):
        """Generates a timestep mapping to local agent timesteps.

        This helps us to keep track of which agent stepped at
        which global timestep.
        Note that the local timestep is given by the index of the
        list for each agent.
        """
        # Only if agent ids have been provided we can build the timestep mapping.
        if len(self.agent_ids) > 0:
            global_t_to_local_t = {agent: IndexMapping() for agent in self.agent_ids}

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

    def _generate_single_agent_episode(
        self,
        agent_id,
        observations: Optional[List[Dict]] = None,
        actions: Optional[List[Dict]] = None,
        rewards: Optional[List[Dict]] = None,
        states: Optional[List[Dict]] = None,
    ):
        """Generates a `SingleAgentEpisode` from multi-agent-data."""

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
            agent_states = (
                None
                if states is None
                else self._get_single_agent_data(
                    agent_id, states, start_index=1, shift=-1
                )
            )
            # TODO (simon): Render images.
            return SingleAgentEpisode(
                observations=agent_observations,
                actions=agent_actions,
                rewards=agent_rewards,
                states=agent_states,
            )
        # Otherwise return None.
        else:
            return None

    def _get_single_agent_data(
        self,
        agent_id: str,
        ma_data: List[Dict],
        start_index: int = 0,
        end_index: int = None,
        shift: int = 0,
    ) -> List[Any]:
        """Returns single agent data from multi-agent data."""
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
