from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.async_vector_env import AsyncVectorEnv


class MultiAgentEnv(object):
    """An environment that hosts multiple independent agents.

    Agents are identified by (string) agent ids.

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
            "car_0": False,
            "car_1": True,
            "__all__": False,
        }
    """

    def reset(self):
        """Resets the env and returns observations from ready agents.
        
        Returns:
            obs (dict): New observations for each ready agent.
        """
        raise NotImplementedError

    def step(self, action_dict):
        """Returns observations from ready agents.

        The returns are dicts mapping from agent_id strings to values. The
        number of agents in the env can vary over time.

        Returns:
            obs (dict): New observations for each ready agent.
            rewards (dict): Reward values for each ready agent. If the
                episode is just started, the value will be None.
            dones (dict): Done values for each ready agent. The special key
                "__all__" is used to indicate env termination.
            infos (dict): Info values for each ready agent.
        """
        raise NotImplementedError


class _MultiAgentEnvToAsync(AsyncVectorEnv):
    """Wraps MultiAgentEnv to implement AsyncVectorEnv.

    This also supports vectorization if num_envs > 1.
    """

    def __init__(self, make_env, existing_envs, num_envs):
        """Wrap existing multi-agent envs.

        Arguments:
            make_env (func|None): Factory that produces a new multiagent env.
                Must be defined if the number of existing envs is less than
                num_envs.
            existing_envs (list): List of existing multiagent envs.
            num_envs (int): Desired num multiagent envs to keep total.
        """
        self.make_env = make_env
        self.envs = existing_envs
        self.num_envs = num_envs
        self.dones = set()
        while len(self.envs) < self.num_envs:
            self.envs.append(self.make_env())
        for env in self.envs:
            assert isinstance(env, MultiAgentEnv)
        self.env_states = [_EnvState(env) for env in self.envs]

    def poll(self):
        obs, rewards, dones, infos = {}, {}, {}, {}
        for i, env_state in enumerate(self.env_states):
            obs[i], rewards[i], dones[i], infos[i] = env_state.poll()
        return obs, rewards, dones, infos, {}

    def send_actions(self, action_dict):
        for env_id, agent_dict in action_dict.items():
            if env_id in self.dones:
                raise ValueError("Env {} is already done".format(env_id))
            env = self.envs[env_id]
            obs, rewards, dones, infos = env.step(agent_dict)
            if dones["__all__"]:
                self.dones.add(env_id)
            self.env_states[env_id].observe(obs, rewards, dones, infos)

    def try_reset(self, env_id):
        obs = self.env_states[env_id].reset()
        if obs is not None:
            self.dones.remove(env_id)
        return obs


class _EnvState(object):
    def __init__(self, env):
        assert isinstance(env, MultiAgentEnv)
        self.env = env
        self.reset()

    def poll(self):
        if self.last_obs is None:
            raise ValueError("Need to send action after polling")
        obs, rew, dones, info = (
            self.last_obs, self.last_rewards, self.last_dones, self.last_infos)
        self.last_obs = None
        self.last_rewards = None
        self.last_dones = None
        self.last_infos = None
        return obs, rew, dones, info

    def observe(self, obs, rewards, dones, infos):
        self.last_obs = obs
        self.last_rewards = rewards
        self.last_dones = dones
        self.last_infos = infos

    def reset(self):
        self.last_obs = self.env.reset()
        self.last_rewards = {
            agent_id: None for agent_id in self.last_obs.keys()}
        self.last_dones = {
            agent_id: False for agent_id in self.last_obs.keys()}
        self.last_infos = {
            agent_id: {} for agent_id in self.last_obs.keys()}
        self.last_dones["__all__"] = False
        return self.last_obs
