from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class AsyncVectorEnv(object):
    """Generalization of VectorEnv to support asynchronous env execution.

    After send_actions(), a dictionary is returned that consists of one or
    more agent results from the environment. Results are identified by unique
    ids. Ids may be reused across episodes.

    AsyncVectorEnv is useful for multi-agent, where multiple agents in the
    environment may come and go over time, and for ServingEnv, which may be
    hosting multiple concurrent user sessions.

    Examples:
        >>> env = MyAsyncVectorEnv()
        >>> obs, rewards, dones, infos = env.poll()
        >>> print(obs)
        {"car_0": [2.4, 1.6], "car_1": [3.4, -3.2]}
        >>> env.send_actions({"car_0": 0, "car_1": 1})
        >>> obs, rewards, dones, infos = env.poll()
        >>> print(obs)
        {"car_0": [4.1, 1.7], "car_1": [3.2, -4.2]}
    """

    def poll(self):
        raise NotImplementedError

    def send_actions(self, action_dict):
        raise NotImplementedError

    def send_error(self, error):
        """Notify that an error has occured."""
        pass

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
        self.vector_width = vector_env.vector_width
        self.new_obs = self.vector_env.vector_reset()
        self.cur_rewards = [None for _ in range(self.vector_width)]
        self.cur_dones = [False for _ in range(self.vector_width)]
        self.cur_infos = [None for _ in range(self.vector_width)]

    def poll(self):
        new_obs = {i: o for i, o in enumerate(self.new_obs)}
        rewards = {i: r for i, r in enumerate(self.cur_rewards)}
        dones = {i: d for i, d in enumerate(self.cur_dones)}
        infos = {i: n for i, n in enumerate(self.cur_infos)}
        self.new_obs = {}
        self.cur_rewards = {}
        self.cur_dones = {}
        self.cur_infos = {}
        return new_obs, rewards, dones, infos

    def send_actions(self, action_dict):
        action_vector = [None] * self.vector_width
        for i in range(self.vector_width):
            action_vector[i] = action_dict[i]
        self.new_obs, self.cur_rewards, self.cur_dones, self.cur_infos = \
            self.vector_env.vector_step(action_vector)

    def try_reset(self, agent_id):
        return self.vector_env.reset_at(agent_id)

    def get_unwrapped(self):
        return self.vector_env.get_unwrapped()
