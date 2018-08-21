from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.env.serving_env import ServingEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class AsyncVectorEnv(object):
    """The lowest-level env interface used by RLlib for sampling.

    AsyncVectorEnv models multiple agents executing asynchronously in multiple
    environments. A call to poll() returns observations from ready agents
    keyed by their environment and agent ids, and actions for those agents
    can be sent back via send_actions().

    All other env types can be adapted to AsyncVectorEnv. RLlib handles these
    conversions internally in PolicyEvaluator, for example:

        gym.Env => rllib.VectorEnv => rllib.AsyncVectorEnv
        rllib.MultiAgentEnv => rllib.AsyncVectorEnv
        rllib.ServingEnv => rllib.AsyncVectorEnv

    Examples:
        >>> env = MyAsyncVectorEnv()
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {
            "env_0": {
                "car_0": [2.4, 1.6],
                "car_1": [3.4, -3.2],
            }
        }
        >>> env.send_actions(
            actions={
                "env_0": {
                    "car_0": 0,
                    "car_1": 1,
                }
            })
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {
            "env_0": {
                "car_0": [4.1, 1.7],
                "car_1": [3.2, -4.2],
            }
        }
        >>> print(dones)
        {
            "env_0": {
                "__all__": False,
                "car_0": False,
                "car_1": True,
            }
        }
    """

    @staticmethod
    def wrap_async(env, make_env=None, num_envs=1):
        """Wraps any env type as needed to expose the async interface."""
        if not isinstance(env, AsyncVectorEnv):
            if isinstance(env, MultiAgentEnv):
                env = _MultiAgentEnvToAsync(
                    make_env=make_env, existing_envs=[env], num_envs=num_envs)
            elif isinstance(env, ServingEnv):
                if num_envs != 1:
                    raise ValueError(
                        "ServingEnv does not currently support num_envs > 1.")
                env = _ServingEnvToAsync(env)
            elif isinstance(env, VectorEnv):
                env = _VectorEnvToAsync(env)
            else:
                env = VectorEnv.wrap(
                    make_env=make_env, existing_envs=[env], num_envs=num_envs)
                env = _VectorEnvToAsync(env)
        assert isinstance(env, AsyncVectorEnv)
        return env

    def poll(self):
        """Returns observations from ready agents.

        The returns are two-level dicts mapping from env_id to a dict of
        agent_id to values. The number of agents and envs can vary over time.

        Returns
        -------
            obs (dict): New observations for each ready agent.
            rewards (dict): Reward values for each ready agent. If the
                episode is just started, the value will be None.
            dones (dict): Done values for each ready agent. The special key
                "__all__" is used to indicate env termination.
            infos (dict): Info values for each ready agent.
            off_policy_actions (dict): Agents may take off-policy actions. When
                that happens, there will be an entry in this dict that contains
                the taken action. There is no need to send_actions() for agents
                that have already chosen off-policy actions.

        """
        raise NotImplementedError

    def send_actions(self, action_dict):
        """Called to send actions back to running agents in this env.

        Actions should be sent for each ready agent that returned observations
        in the previous poll() call.

        Arguments:
            action_dict (dict): Actions values keyed by env_id and agent_id.
        """
        raise NotImplementedError

    def try_reset(self, env_id):
        """Attempt to reset the env with the given id.

        If the environment does not support synchronous reset, None can be
        returned here.

        Returns:
            obs (dict|None): Resetted observation or None if not supported.
        """
        return None

    def get_unwrapped(self):
        """Return a reference to some underlying gym env, if any.

        Returns:
            env (gym.Env|None): Underlying gym env or None.
        """
        return None


# Fixed agent identifier when there is only the single agent in the env
_DUMMY_AGENT_ID = "single_agent"


def _with_dummy_agent_id(env_id_to_values, dummy_id=_DUMMY_AGENT_ID):
    return {k: {dummy_id: v} for (k, v) in env_id_to_values.items()}


class _ServingEnvToAsync(AsyncVectorEnv):
    """Internal adapter of ServingEnv to AsyncVectorEnv."""

    def __init__(self, serving_env):
        self.serving_env = serving_env
        serving_env.start()

    def poll(self):
        with self.serving_env._results_avail_condition:
            results = self._poll()
            while len(results[0]) == 0:
                self.serving_env._results_avail_condition.wait()
                results = self._poll()
                if not self.serving_env.isAlive():
                    raise Exception("Serving thread has stopped.")
        limit = self.serving_env._max_concurrent_episodes
        assert len(results[0]) < limit, \
            ("Too many concurrent episodes, were some leaked? This ServingEnv "
             "was created with max_concurrent={}".format(limit))
        return results

    def _poll(self):
        all_obs, all_rewards, all_dones, all_infos = {}, {}, {}, {}
        off_policy_actions = {}
        for eid, episode in self.serving_env._episodes.copy().items():
            data = episode.get_data()
            if episode.cur_done:
                del self.serving_env._episodes[eid]
            if data:
                all_obs[eid] = data["obs"]
                all_rewards[eid] = data["reward"]
                all_dones[eid] = data["done"]
                all_infos[eid] = data["info"]
                if "off_policy_action" in data:
                    off_policy_actions[eid] = data["off_policy_action"]
        return _with_dummy_agent_id(all_obs), \
            _with_dummy_agent_id(all_rewards), \
            _with_dummy_agent_id(all_dones, "__all__"), \
            _with_dummy_agent_id(all_infos), \
            _with_dummy_agent_id(off_policy_actions)

    def send_actions(self, action_dict):
        for eid, action in action_dict.items():
            self.serving_env._episodes[eid].action_queue.put(
                action[_DUMMY_AGENT_ID])


class _VectorEnvToAsync(AsyncVectorEnv):
    """Internal adapter of VectorEnv to AsyncVectorEnv.

    We assume the caller will always send the full vector of actions in each
    call to send_actions(), and that they call reset_at() on all completed
    environments before calling send_actions().
    """

    def __init__(self, vector_env):
        self.vector_env = vector_env
        self.num_envs = vector_env.num_envs
        self.new_obs = self.vector_env.vector_reset()
        self.cur_rewards = [None for _ in range(self.num_envs)]
        self.cur_dones = [False for _ in range(self.num_envs)]
        self.cur_infos = [None for _ in range(self.num_envs)]

    def poll(self):
        new_obs = dict(enumerate(self.new_obs))
        rewards = dict(enumerate(self.cur_rewards))
        dones = dict(enumerate(self.cur_dones))
        infos = dict(enumerate(self.cur_infos))
        self.new_obs = []
        self.cur_rewards = []
        self.cur_dones = []
        self.cur_infos = []
        return _with_dummy_agent_id(new_obs), \
            _with_dummy_agent_id(rewards), \
            _with_dummy_agent_id(dones, "__all__"), \
            _with_dummy_agent_id(infos), {}

    def send_actions(self, action_dict):
        action_vector = [None] * self.num_envs
        for i in range(self.num_envs):
            action_vector[i] = action_dict[i][_DUMMY_AGENT_ID]
        self.new_obs, self.cur_rewards, self.cur_dones, self.cur_infos = \
            self.vector_env.vector_step(action_vector)

    def try_reset(self, env_id):
        return {_DUMMY_AGENT_ID: self.vector_env.reset_at(env_id)}

    def get_unwrapped(self):
        return self.vector_env.get_unwrapped()


class _MultiAgentEnvToAsync(AsyncVectorEnv):
    """Internal adapter of MultiAgentEnv to AsyncVectorEnv.

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
            self.envs.append(self.make_env(len(self.envs)))
        for env in self.envs:
            assert isinstance(env, MultiAgentEnv)
        self.env_states = [_MultiAgentEnvState(env) for env in self.envs]

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
        if obs is not None and env_id in self.dones:
            self.dones.remove(env_id)
        return obs


class _MultiAgentEnvState(object):
    def __init__(self, env):
        assert isinstance(env, MultiAgentEnv)
        self.env = env
        self.reset()

    def poll(self):
        obs, rew, dones, info = (self.last_obs, self.last_rewards,
                                 self.last_dones, self.last_infos)
        self.last_obs = {}
        self.last_rewards = {}
        self.last_dones = {"__all__": False}
        self.last_infos = {}
        return obs, rew, dones, info

    def observe(self, obs, rewards, dones, infos):
        self.last_obs = obs
        self.last_rewards = rewards
        self.last_dones = dones
        self.last_infos = infos

    def reset(self):
        self.last_obs = self.env.reset()
        self.last_rewards = {
            agent_id: None
            for agent_id in self.last_obs.keys()
        }
        self.last_dones = {
            agent_id: False
            for agent_id in self.last_obs.keys()
        }
        self.last_infos = {agent_id: {} for agent_id in self.last_obs.keys()}
        self.last_dones["__all__"] = False
        return self.last_obs
