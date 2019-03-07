from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import override, PublicAPI


@PublicAPI
class BaseEnv(object):
    """The lowest-level env interface used by RLlib for sampling.

    BaseEnv models multiple agents executing asynchronously in multiple
    environments. A call to poll() returns observations from ready agents
    keyed by their environment and agent ids, and actions for those agents
    can be sent back via send_actions().

    All other env types can be adapted to BaseEnv. RLlib handles these
    conversions internally in PolicyEvaluator, for example:

        gym.Env => rllib.VectorEnv => rllib.BaseEnv
        rllib.MultiAgentEnv => rllib.BaseEnv
        rllib.ExternalEnv => rllib.BaseEnv

    Attributes:
        action_space (gym.Space): Action space. This must be defined for
            single-agent envs. Multi-agent envs can set this to None.
        observation_space (gym.Space): Observation space. This must be defined
            for single-agent envs. Multi-agent envs can set this to None.

    Examples:
        >>> env = MyBaseEnv()
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {
            "env_0": {
                "car_0": [2.4, 1.6],
                "car_1": [3.4, -3.2],
            },
            "env_1": {
                "car_0": [8.0, 4.1],
            },
            "env_2": {
                "car_0": [2.3, 3.3],
                "car_1": [1.4, -0.2],
                "car_3": [1.2, 0.1],
            },
        }
        >>> env.send_actions(
            actions={
                "env_0": {
                    "car_0": 0,
                    "car_1": 1,
                }, ...
            })
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {
            "env_0": {
                "car_0": [4.1, 1.7],
                "car_1": [3.2, -4.2],
            }, ...
        }
        >>> print(dones)
        {
            "env_0": {
                "__all__": False,
                "car_0": False,
                "car_1": True,
            }, ...
        }
    """

    @staticmethod
    def to_base_env(env,
                    make_env=None,
                    num_envs=1,
                    remote_envs=False,
                    async_remote_envs=False):
        """Wraps any env type as needed to expose the async interface."""

        from ray.rllib.env.remote_vector_env import RemoteVectorEnv
        if (remote_envs or async_remote_envs) and num_envs == 1:
            raise ValueError(
                "Remote envs only make sense to use if num_envs > 1 "
                "(i.e. vectorization is enabled).")
        if remote_envs and async_remote_envs:
            raise ValueError("You can only specify one of remote_envs or "
                             "async_remote_envs.")

        if not isinstance(env, BaseEnv):
            if isinstance(env, MultiAgentEnv):
                if remote_envs:
                    env = RemoteVectorEnv(
                        make_env, num_envs, multiagent=True, sync=True)
                elif async_remote_envs:
                    env = RemoteVectorEnv(
                        make_env, num_envs, multiagent=True, sync=False)
                else:
                    env = _MultiAgentEnvToBaseEnv(
                        make_env=make_env,
                        existing_envs=[env],
                        num_envs=num_envs)
            elif isinstance(env, ExternalEnv):
                if num_envs != 1:
                    raise ValueError(
                        "ExternalEnv does not currently support num_envs > 1.")
                env = _ExternalEnvToBaseEnv(env)
            elif isinstance(env, VectorEnv):
                env = _VectorEnvToBaseEnv(env)
            else:
                if remote_envs:
                    env = RemoteVectorEnv(
                        make_env, num_envs, multiagent=False, sync=True)
                elif async_remote_envs:
                    env = RemoteVectorEnv(
                        make_env, num_envs, multiagent=False, sync=False)
                else:
                    env = VectorEnv.wrap(
                        make_env=make_env,
                        existing_envs=[env],
                        num_envs=num_envs,
                        action_space=env.action_space,
                        observation_space=env.observation_space)
                    env = _VectorEnvToBaseEnv(env)
        assert isinstance(env, BaseEnv), env
        return env

    @PublicAPI
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

    @PublicAPI
    def send_actions(self, action_dict):
        """Called to send actions back to running agents in this env.

        Actions should be sent for each ready agent that returned observations
        in the previous poll() call.

        Arguments:
            action_dict (dict): Actions values keyed by env_id and agent_id.
        """
        raise NotImplementedError

    @PublicAPI
    def try_reset(self, env_id):
        """Attempt to reset the env with the given id.

        If the environment does not support synchronous reset, None can be
        returned here.

        Returns:
            obs (dict|None): Resetted observation or None if not supported.
        """
        return None

    @PublicAPI
    def get_unwrapped(self):
        """Return a reference to the underlying gym envs, if any.

        Returns:
            envs (list): Underlying gym envs or [].
        """
        return []


# Fixed agent identifier when there is only the single agent in the env
_DUMMY_AGENT_ID = "single_agent"


def _with_dummy_agent_id(env_id_to_values, dummy_id=_DUMMY_AGENT_ID):
    return {k: {dummy_id: v} for (k, v) in env_id_to_values.items()}


class _ExternalEnvToBaseEnv(BaseEnv):
    """Internal adapter of ExternalEnv to BaseEnv."""

    def __init__(self, external_env, preprocessor=None):
        self.external_env = external_env
        self.prep = preprocessor
        self.action_space = external_env.action_space
        if preprocessor:
            self.observation_space = preprocessor.observation_space
        else:
            self.observation_space = external_env.observation_space
        external_env.start()

    @override(BaseEnv)
    def poll(self):
        with self.external_env._results_avail_condition:
            results = self._poll()
            while len(results[0]) == 0:
                self.external_env._results_avail_condition.wait()
                results = self._poll()
                if not self.external_env.isAlive():
                    raise Exception("Serving thread has stopped.")
        limit = self.external_env._max_concurrent_episodes
        assert len(results[0]) < limit, \
            ("Too many concurrent episodes, were some leaked? This "
             "ExternalEnv was created with max_concurrent={}".format(limit))
        return results

    @override(BaseEnv)
    def send_actions(self, action_dict):
        for eid, action in action_dict.items():
            self.external_env._episodes[eid].action_queue.put(
                action[_DUMMY_AGENT_ID])

    def _poll(self):
        all_obs, all_rewards, all_dones, all_infos = {}, {}, {}, {}
        off_policy_actions = {}
        for eid, episode in self.external_env._episodes.copy().items():
            data = episode.get_data()
            if episode.cur_done:
                del self.external_env._episodes[eid]
            if data:
                if self.prep:
                    all_obs[eid] = self.prep.transform(data["obs"])
                else:
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


class _VectorEnvToBaseEnv(BaseEnv):
    """Internal adapter of VectorEnv to BaseEnv.

    We assume the caller will always send the full vector of actions in each
    call to send_actions(), and that they call reset_at() on all completed
    environments before calling send_actions().
    """

    def __init__(self, vector_env):
        self.vector_env = vector_env
        self.action_space = vector_env.action_space
        self.observation_space = vector_env.observation_space
        self.num_envs = vector_env.num_envs
        self.new_obs = None  # lazily initialized
        self.cur_rewards = [None for _ in range(self.num_envs)]
        self.cur_dones = [False for _ in range(self.num_envs)]
        self.cur_infos = [None for _ in range(self.num_envs)]

    @override(BaseEnv)
    def poll(self):
        if self.new_obs is None:
            self.new_obs = self.vector_env.vector_reset()
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

    @override(BaseEnv)
    def send_actions(self, action_dict):
        action_vector = [None] * self.num_envs
        for i in range(self.num_envs):
            action_vector[i] = action_dict[i][_DUMMY_AGENT_ID]
        self.new_obs, self.cur_rewards, self.cur_dones, self.cur_infos = \
            self.vector_env.vector_step(action_vector)

    @override(BaseEnv)
    def try_reset(self, env_id):
        return {_DUMMY_AGENT_ID: self.vector_env.reset_at(env_id)}

    @override(BaseEnv)
    def get_unwrapped(self):
        return self.vector_env.get_unwrapped()


class _MultiAgentEnvToBaseEnv(BaseEnv):
    """Internal adapter of MultiAgentEnv to BaseEnv.

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

    @override(BaseEnv)
    def poll(self):
        obs, rewards, dones, infos = {}, {}, {}, {}
        for i, env_state in enumerate(self.env_states):
            obs[i], rewards[i], dones[i], infos[i] = env_state.poll()
        return obs, rewards, dones, infos, {}

    @override(BaseEnv)
    def send_actions(self, action_dict):
        for env_id, agent_dict in action_dict.items():
            if env_id in self.dones:
                raise ValueError("Env {} is already done".format(env_id))
            env = self.envs[env_id]
            obs, rewards, dones, infos = env.step(agent_dict)
            assert isinstance(obs, dict), "Not a multi-agent obs"
            assert isinstance(rewards, dict), "Not a multi-agent reward"
            assert isinstance(dones, dict), "Not a multi-agent return"
            assert isinstance(infos, dict), "Not a multi-agent info"
            if set(obs.keys()) != set(rewards.keys()):
                raise ValueError(
                    "Key set for obs and rewards must be the same: "
                    "{} vs {}".format(obs.keys(), rewards.keys()))
            if set(infos).difference(set(obs)):
                raise ValueError("Key set for infos must be a subset of obs: "
                                 "{} vs {}".format(infos.keys(), obs.keys()))
            if "__all__" not in dones:
                raise ValueError(
                    "In multi-agent environments, '__all__': True|False must "
                    "be included in the 'done' dict: got {}.".format(dones))
            if dones["__all__"]:
                self.dones.add(env_id)
            self.env_states[env_id].observe(obs, rewards, dones, infos)

    @override(BaseEnv)
    def try_reset(self, env_id):
        obs = self.env_states[env_id].reset()
        assert isinstance(obs, dict), "Not a multi-agent obs"
        if obs is not None and env_id in self.dones:
            self.dones.remove(env_id)
        return obs

    @override(BaseEnv)
    def get_unwrapped(self):
        return [state.env for state in self.env_states]


class _MultiAgentEnvState(object):
    def __init__(self, env):
        assert isinstance(env, MultiAgentEnv)
        self.env = env
        self.initialized = False

    def poll(self):
        if not self.initialized:
            self.reset()
            self.initialized = True
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
