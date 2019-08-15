from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class MultiAgentEnv(object):
    """An environment that hosts multiple independent agents.

    Agents are identified by (string) agent ids. Note that these "agents" here
    are not to be confused with RLlib agents.

    Examples:
        >>> env = MyMultiAgentEnv()
        >>> obs = env.reset()
        >>> print(obs)
        {
            "car_0": [2.4, 1.6],
            "car_1": [3.4, -3.2],
            "traffic_light_1": [0, 3, 5, 1],
        }
        >>> obs, rewards, dones, infos = env.step(
            action_dict={
                "car_0": 1, "car_1": 0, "traffic_light_1": 2,
            })
        >>> print(rewards)
        {
            "car_0": 3,
            "car_1": -1,
            "traffic_light_1": 0,
        }
        >>> print(dones)
        {
            "car_0": False,    # car_0 is still running
            "car_1": True,     # car_1 is done
            "__all__": False,  # the env is not done
        }
        >>> print(infos)
        {
            "car_0": {},  # info for car_0
            "car_1": {},  # info for car_1
        }
    """

    @PublicAPI
    def reset(self):
        """Resets the env and returns observations from ready agents.

        Returns:
            obs (dict): New observations for each ready agent.
        """
        raise NotImplementedError

    @PublicAPI
    def step(self, action_dict):
        """Returns observations from ready agents.

        The returns are dicts mapping from agent_id strings to values. The
        number of agents in the env can vary over time.

        Returns
        -------
            obs (dict): New observations for each ready agent.
            rewards (dict): Reward values for each ready agent. If the
                episode is just started, the value will be None.
            dones (dict): Done values for each ready agent. The special key
                "__all__" (required) is used to indicate env termination.
            infos (dict): Optional info values for each agent id.
        """
        raise NotImplementedError

# yapf: disable
# __grouping_doc_begin__
    @PublicAPI
    def with_agent_groups(self, groups, obs_space=None, act_space=None):
        """Convenience method for grouping together agents in this env.

        An agent group is a list of agent ids that are mapped to a single
        logical agent. All agents of the group must act at the same time in the
        environment. The grouped agent exposes Tuple action and observation
        spaces that are the concatenated action and obs spaces of the
        individual agents.

        The rewards of all the agents in a group are summed. The individual
        agent rewards are available under the "individual_rewards" key of the
        group info return.

        Agent grouping is required to leverage algorithms such as Q-Mix.

        This API is experimental.

        Arguments:
            groups (dict): Mapping from group id to a list of the agent ids
                of group members. If an agent id is not present in any group
                value, it will be left ungrouped.
            obs_space (Space): Optional observation space for the grouped
                env. Must be a tuple space.
            act_space (Space): Optional action space for the grouped env.
                Must be a tuple space.

        Examples:
            >>> env = YourMultiAgentEnv(...)
            >>> grouped_env = env.with_agent_groups(env, {
            ...   "group1": ["agent1", "agent2", "agent3"],
            ...   "group2": ["agent4", "agent5"],
            ... })
        """

        from ray.rllib.env.group_agents_wrapper import _GroupAgentsWrapper
        return _GroupAgentsWrapper(self, groups, obs_space, act_space)
# __grouping_doc_end__
# yapf: enable
