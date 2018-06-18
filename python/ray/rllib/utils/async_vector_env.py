from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class AsyncVectorEnv(object):
    """The lowest-level env interface used by RLlib for sampling.

    AsyncVectorEnv models multiple agents executing asynchronously. A call to
    poll() returns observations from ready agents, and actions for those agents
    can be sent back via send_actions().

    All other env types can be adapted to AsyncVectorEnv. RLlib handles these
    conversions internally in CommonPolicyEvaluator, for example:

        gym.Env => rllib.VectorEnv => rllib.AsyncVectorEnv
        rllib.ServingEnv => rllib.AsyncVectorEnv

    Examples:
        >>> env = MyAsyncVectorEnv()
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {"car_0": [2.4, 1.6], "car_1": [3.4, -3.2]}
        >>> env.send_actions({"car_0": 0, "car_1": 1})
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {"car_0": [4.1, 1.7], "car_1": [3.2, -4.2]}
    """

    def poll(self):
        """Returns observations from ready agents.

        The returns are dicts mapping from agent episode ids to values. The
        number of agents can vary over time.

        Returns:
            obs (dict): New observations for each ready episode.
            rewards (dict): Reward values for each ready episode. If the
                episode is just started, the value will be None.
            dones (dict): Done values for each ready episode. If True, the
                episode is terminated.
            infos (dict): Info values for each ready episode.
            off_policy_actions (dict): Agents may take off-policy actions. When
                that happens, there will be an entry in this dict that contains
                the taken action.
        """
        raise NotImplementedError

    def send_actions(self, action_dict):
        """Called to send actions back to running agents in this env.

        Arguments:
            action_dict (dict): Actions for each agent to take.
        """
        raise NotImplementedError

    def try_reset(self, agent_id):
        """Attempt to reset the agent with the given id.

        If the environment does not support synchronous reset, None can be
        returned here.

        Returns:
            obs (obj|None): Resetted observation or None if not supported.
        """
        return None

    def get_unwrapped(self):
        """Return a reference to some underlying gym env, if any.

        Returns:
            env (gym.Env|None): Underlying gym env or None.
        """
        return None


class _VectorEnvToAsync(AsyncVectorEnv):
    """Wraps VectorEnv to implement AsyncVectorEnv.

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
        return new_obs, rewards, dones, infos, {}

    def send_actions(self, action_dict):
        action_vector = [None] * self.num_envs
        for i in range(self.num_envs):
            action_vector[i] = action_dict[i]
        self.new_obs, self.cur_rewards, self.cur_dones, self.cur_infos = \
            self.vector_env.vector_step(action_vector)

    def try_reset(self, agent_id):
        return self.vector_env.reset_at(agent_id)

    def get_unwrapped(self):
        return self.vector_env.get_unwrapped()
