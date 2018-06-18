from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class AsyncVectorEnv(object):
    """The lowest-level env interface used by RLlib for sampling.

    AsyncVectorEnv models multiple agents executing asynchronously in multiple
    environments. A call to poll() returns observations from ready agents
    keyed by their environment and agent ids, and actions for those agents
    can be sent back via send_actions().

    All other env types can be adapted to AsyncVectorEnv. RLlib handles these
    conversions internally in CommonPolicyEvaluator, for example:

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

    def poll(self):
        """Returns observations from ready agents.

        The returns are two-level dicts mapping from env_id to a dict of
        agent_id to values. The number of agents and envs can vary over time.

        Returns:
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
