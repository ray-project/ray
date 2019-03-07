from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import random

import numpy as np

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class MultiAgentEpisode(object):
    """Tracks the current state of a (possibly multi-agent) episode.

    Attributes:
        new_batch_builder (func): Create a new MultiAgentSampleBatchBuilder.
        add_extra_batch (func): Return a built MultiAgentBatch to the sampler.
        batch_builder (obj): Batch builder for the current episode.
        total_reward (float): Summed reward across all agents in this episode.
        length (int): Length of this episode.
        episode_id (int): Unique id identifying this trajectory.
        agent_rewards (dict): Summed rewards broken down by agent.
        custom_metrics (dict): Dict where the you can add custom metrics.
        user_data (dict): Dict that you can use for temporary storage.

    Use case 1: Model-based rollouts in multi-agent:
        A custom compute_actions() function in a policy graph can inspect the
        current episode state and perform a number of rollouts based on the
        policies and state of other agents in the environment.

    Use case 2: Returning extra rollouts data.
        The model rollouts can be returned back to the sampler by calling:

        >>> batch = episode.new_batch_builder()
        >>> for each transition:
               batch.add_values(...)  # see sampler for usage
        >>> episode.extra_batches.add(batch.build_and_reset())
    """

    def __init__(self, policies, policy_mapping_fn, batch_builder_factory,
                 extra_batch_callback):
        self.new_batch_builder = batch_builder_factory
        self.add_extra_batch = extra_batch_callback
        self.batch_builder = batch_builder_factory()
        self.total_reward = 0.0
        self.length = 0
        self.episode_id = random.randrange(2e9)
        self.agent_rewards = defaultdict(float)
        self.custom_metrics = {}
        self.user_data = {}
        self._policies = policies
        self._policy_mapping_fn = policy_mapping_fn
        self._next_agent_index = 0
        self._agent_to_index = {}
        self._agent_to_policy = {}
        self._agent_to_rnn_state = {}
        self._agent_to_last_obs = {}
        self._agent_to_last_raw_obs = {}
        self._agent_to_last_info = {}
        self._agent_to_last_action = {}
        self._agent_to_last_pi_info = {}
        self._agent_to_prev_action = {}
        self._agent_reward_history = defaultdict(list)

    @DeveloperAPI
    def policy_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the policy graph for the specified agent.

        If the agent is new, the policy mapping fn will be called to bind the
        agent to a policy for the duration of the episode.
        """

        if agent_id not in self._agent_to_policy:
            self._agent_to_policy[agent_id] = self._policy_mapping_fn(agent_id)
        return self._agent_to_policy[agent_id]

    @DeveloperAPI
    def last_observation_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the last observation for the specified agent."""

        return self._agent_to_last_obs.get(agent_id)

    @DeveloperAPI
    def last_raw_obs_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the last un-preprocessed obs for the specified agent."""

        return self._agent_to_last_raw_obs.get(agent_id)

    @DeveloperAPI
    def last_info_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the last info for the specified agent."""

        return self._agent_to_last_info.get(agent_id)

    @DeveloperAPI
    def last_action_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the last action for the specified agent, or zeros."""

        if agent_id in self._agent_to_last_action:
            return _flatten_action(self._agent_to_last_action[agent_id])
        else:
            policy = self._policies[self.policy_for(agent_id)]
            flat = _flatten_action(policy.action_space.sample())
            return np.zeros_like(flat)

    @DeveloperAPI
    def prev_action_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the previous action for the specified agent."""

        if agent_id in self._agent_to_prev_action:
            return _flatten_action(self._agent_to_prev_action[agent_id])
        else:
            # We're at t=0, so return all zeros.
            return np.zeros_like(self.last_action_for(agent_id))

    @DeveloperAPI
    def prev_reward_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the previous reward for the specified agent."""

        history = self._agent_reward_history[agent_id]
        if len(history) >= 2:
            return history[-2]
        else:
            # We're at t=0, so there is no previous reward, just return zero.
            return 0.0

    @DeveloperAPI
    def rnn_state_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the last RNN state for the specified agent."""

        if agent_id not in self._agent_to_rnn_state:
            policy = self._policies[self.policy_for(agent_id)]
            self._agent_to_rnn_state[agent_id] = policy.get_initial_state()
        return self._agent_to_rnn_state[agent_id]

    @DeveloperAPI
    def last_pi_info_for(self, agent_id=_DUMMY_AGENT_ID):
        """Returns the last info object for the specified agent."""

        return self._agent_to_last_pi_info[agent_id]

    def _add_agent_rewards(self, reward_dict):
        for agent_id, reward in reward_dict.items():
            if reward is not None:
                self.agent_rewards[agent_id,
                                   self.policy_for(agent_id)] += reward
                self.total_reward += reward
                self._agent_reward_history[agent_id].append(reward)

    def _set_rnn_state(self, agent_id, rnn_state):
        self._agent_to_rnn_state[agent_id] = rnn_state

    def _set_last_observation(self, agent_id, obs):
        self._agent_to_last_obs[agent_id] = obs

    def _set_last_raw_obs(self, agent_id, obs):
        self._agent_to_last_raw_obs[agent_id] = obs

    def _set_last_info(self, agent_id, info):
        self._agent_to_last_info[agent_id] = info

    def _set_last_action(self, agent_id, action):
        if agent_id in self._agent_to_last_action:
            self._agent_to_prev_action[agent_id] = \
                self._agent_to_last_action[agent_id]
        self._agent_to_last_action[agent_id] = action

    def _set_last_pi_info(self, agent_id, pi_info):
        self._agent_to_last_pi_info[agent_id] = pi_info

    def _agent_index(self, agent_id):
        if agent_id not in self._agent_to_index:
            self._agent_to_index[agent_id] = self._next_agent_index
            self._next_agent_index += 1
        return self._agent_to_index[agent_id]


def _flatten_action(action):
    # Concatenate tuple actions
    if isinstance(action, list) or isinstance(action, tuple):
        expanded = []
        for a in action:
            if not hasattr(a, "shape") or len(a.shape) == 0:
                expanded.append(np.expand_dims(a, 1))
            else:
                expanded.append(a)
        action = np.concatenate(expanded, axis=0).flatten()
    return action
