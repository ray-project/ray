from typing import Tuple, Dict, List
import gym

from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import MultiAgentDict, AgentID

# If the obs space is Dict type, look for the global state under this key.
ENV_STATE = "state"


@PublicAPI
class MultiAgentEnv:
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
        ...    action_dict={
        ...        "car_0": 1, "car_1": 0, "traffic_light_1": 2,
        ...    })
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
    def reset(self) -> MultiAgentDict:
        """Resets the env and returns observations from ready agents.

        Returns:
            obs (dict): New observations for each ready agent.
        """
        raise NotImplementedError

    @PublicAPI
    def step(
            self, action_dict: MultiAgentDict
    ) -> Tuple[MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict]:
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
    def with_agent_groups(
            self,
            groups: Dict[str, List[AgentID]],
            obs_space: gym.Space = None,
            act_space: gym.Space = None) -> "MultiAgentEnv":
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

        Args:
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

        from ray.rllib.env.wrappers.group_agents_wrapper import \
            GroupAgentsWrapper
        return GroupAgentsWrapper(self, groups, obs_space, act_space)
# __grouping_doc_end__
# yapf: enable


def make_multi_agent(env_name_or_creator):
    """Convenience wrapper for any sigle-agent env to be converted into MA.

    Agent IDs are int numbers starting from 0 (first agent).

    Args:
        env_name_or_creator (Union[str, Callable[]]: String specifier or
            env_maker function.

    Returns:
        Type[MultiAgentEnv]: New MultiAgentEnv class to be used as env.
            The constructor takes a config dict with `num_agents` key
            (default=1). The reset of the config dict will be passed on to the
            underlying single-agent env's constructor.

    Examples:
         >>> # By gym string:
         >>> ma_cartpole_cls = make_multi_agent("CartPole-v0")
         >>> # Create a 2 agent multi-agent cartpole.
         >>> ma_cartpole = ma_cartpole_cls({"num_agents": 2})
         >>> obs = ma_cartpole.reset()
         >>> print(obs)
         ... {0: [...], 1: [...]}

         >>> # By env-maker callable:
         >>> ma_stateless_cartpole_cls = make_multi_agent(
         ...    lambda config: StatelessCartPole(config))
         >>> # Create a 2 agent multi-agent stateless cartpole.
         >>> ma_stateless_cartpole = ma_stateless_cartpole_cls(
         ...    {"num_agents": 2})
    """

    class MultiEnv(MultiAgentEnv):
        def __init__(self, config):
            num = config.pop("num_agents", 1)
            if isinstance(env_name_or_creator, str):
                self.agents = [
                    gym.make(env_name_or_creator) for _ in range(num)
                ]
            else:
                self.agents = [env_name_or_creator(config) for _ in range(num)]
            self.dones = set()
            self.observation_space = self.agents[0].observation_space
            self.action_space = self.agents[0].action_space

        def reset(self):
            self.dones = set()
            return {i: a.reset() for i, a in enumerate(self.agents)}

        def step(self, action_dict):
            obs, rew, done, info = {}, {}, {}, {}
            for i, action in action_dict.items():
                obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
                if done[i]:
                    self.dones.add(i)
            done["__all__"] = len(self.dones) == len(self.agents)
            return obs, rew, done, info

    return MultiEnv
