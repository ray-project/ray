import logging
from typing import Callable, Tuple, Optional, List, Dict, Any, TYPE_CHECKING, Union, Set

import gymnasium as gym
import ray
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.typing import AgentID, EnvID, EnvType, MultiEnvDict

if TYPE_CHECKING:
    from ray.rllib.evaluation.rollout_worker import RolloutWorker

ASYNC_RESET_RETURN = "async_reset_return"

logger = logging.getLogger(__name__)


@OldAPIStack
class BaseEnv:
    """The lowest-level env interface used by RLlib for sampling.

    BaseEnv models multiple agents executing asynchronously in multiple
    vectorized sub-environments. A call to `poll()` returns observations from
    ready agents keyed by their sub-environment ID and agent IDs, and
    actions for those agents can be sent back via `send_actions()`.

    All other RLlib supported env types can be converted to BaseEnv.
    RLlib handles these conversions internally in RolloutWorker, for example:

    gym.Env => rllib.VectorEnv => rllib.BaseEnv
    rllib.MultiAgentEnv (is-a gym.Env) => rllib.VectorEnv => rllib.BaseEnv
    rllib.ExternalEnv => rllib.BaseEnv

    .. testcode::
        :skipif: True

        MyBaseEnv = ...
        env = MyBaseEnv()
        obs, rewards, terminateds, truncateds, infos, off_policy_actions = (
            env.poll()
        )
        print(obs)

        env.send_actions({
          "env_0": {
            "car_0": 0,
            "car_1": 1,
          }, ...
        })
        obs, rewards, terminateds, truncateds, infos, off_policy_actions = (
            env.poll()
        )
        print(obs)

        print(terminateds)

    .. testoutput::

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
        {
            "env_0": {
                "car_0": [4.1, 1.7],
                "car_1": [3.2, -4.2],
            }, ...
        }
        {
            "env_0": {
                "__all__": False,
                "car_0": False,
                "car_1": True,
            }, ...
        }

    """

    def to_base_env(
        self,
        make_env: Optional[Callable[[int], EnvType]] = None,
        num_envs: int = 1,
        remote_envs: bool = False,
        remote_env_batch_wait_ms: int = 0,
        restart_failed_sub_environments: bool = False,
    ) -> "BaseEnv":
        """Converts an RLlib-supported env into a BaseEnv object.

        Supported types for the `env` arg are gym.Env, BaseEnv,
        VectorEnv, MultiAgentEnv, ExternalEnv, or ExternalMultiAgentEnv.

        The resulting BaseEnv is always vectorized (contains n
        sub-environments) to support batched forward passes, where n may also
        be 1. BaseEnv also supports async execution via the `poll` and
        `send_actions` methods and thus supports external simulators.

        TODO: Support gym3 environments, which are already vectorized.

        Args:
            env: An already existing environment of any supported env type
                to convert/wrap into a BaseEnv. Supported types are gym.Env,
                BaseEnv, VectorEnv, MultiAgentEnv, ExternalEnv, and
                ExternalMultiAgentEnv.
            make_env: A callable taking an int as input (which indicates the
                number of individual sub-environments within the final
                vectorized BaseEnv) and returning one individual
                sub-environment.
            num_envs: The number of sub-environments to create in the
                resulting (vectorized) BaseEnv. The already existing `env`
                will be one of the `num_envs`.
            remote_envs: Whether each sub-env should be a @ray.remote actor.
                You can set this behavior in your config via the
                `remote_worker_envs=True` option.
            remote_env_batch_wait_ms: The wait time (in ms) to poll remote
                sub-environments for, if applicable. Only used if
                `remote_envs` is True.
            policy_config: Optional policy config dict.

        Returns:
            The resulting BaseEnv object.
        """
        return self

    def poll(
        self,
    ) -> Tuple[
        MultiEnvDict,
        MultiEnvDict,
        MultiEnvDict,
        MultiEnvDict,
        MultiEnvDict,
        MultiEnvDict,
    ]:
        """Returns observations from ready agents.

        All return values are two-level dicts mapping from EnvID to dicts
        mapping from AgentIDs to (observation/reward/etc..) values.
        The number of agents and sub-environments may vary over time.

        Returns:
            Tuple consisting of:
            New observations for each ready agent.
            Reward values for each ready agent. If the episode is just started,
            the value will be None.
            Terminated values for each ready agent. The special key "__all__" is used to
            indicate episode termination.
            Truncated values for each ready agent. The special key "__all__"
            is used to indicate episode truncation.
            Info values for each ready agent.
            Agents may take off-policy actions, in which case, there will be an entry
            in this dict that contains the taken action. There is no need to
            `send_actions()` for agents that have already chosen off-policy actions.
        """
        raise NotImplementedError

    def send_actions(self, action_dict: MultiEnvDict) -> None:
        """Called to send actions back to running agents in this env.

        Actions should be sent for each ready agent that returned observations
        in the previous poll() call.

        Args:
            action_dict: Actions values keyed by env_id and agent_id.
        """
        raise NotImplementedError

    def try_reset(
        self,
        env_id: Optional[EnvID] = None,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[Optional[MultiEnvDict], Optional[MultiEnvDict]]:
        """Attempt to reset the sub-env with the given id or all sub-envs.

        If the environment does not support synchronous reset, a tuple of
        (ASYNC_RESET_REQUEST, ASYNC_RESET_REQUEST) can be returned here.

        Note: A MultiAgentDict is returned when using the deprecated wrapper
        classes such as `ray.rllib.env.base_env._MultiAgentEnvToBaseEnv`,
        however for consistency with the poll() method, a `MultiEnvDict` is
        returned from the new wrapper classes, such as
        `ray.rllib.env.multi_agent_env.MultiAgentEnvWrapper`.

        Args:
            env_id: The sub-environment's ID if applicable. If None, reset
                the entire Env (i.e. all sub-environments).
            seed: The seed to be passed to the sub-environment(s) when
                resetting it. If None, will not reset any existing PRNG. If you pass an
                integer, the PRNG will be reset even if it already exists.
            options: An options dict to be passed to the sub-environment(s) when
                resetting it.

        Returns:
            A tuple consisting of a) the reset (multi-env/multi-agent) observation
            dict and b) the reset (multi-env/multi-agent) infos dict. Returns the
            (ASYNC_RESET_REQUEST, ASYNC_RESET_REQUEST) tuple, if not supported.
        """
        return None, None

    def try_restart(self, env_id: Optional[EnvID] = None) -> None:
        """Attempt to restart the sub-env with the given id or all sub-envs.

        This could result in the sub-env being completely removed (gc'd) and recreated.

        Args:
            env_id: The sub-environment's ID, if applicable. If None, restart
                the entire Env (i.e. all sub-environments).
        """
        return None

    def get_sub_environments(self, as_dict: bool = False) -> Union[List[EnvType], dict]:
        """Return a reference to the underlying sub environments, if any.

        Args:
            as_dict: If True, return a dict mapping from env_id to env.

        Returns:
            List or dictionary of the underlying sub environments or [] / {}.
        """
        if as_dict:
            return {}
        return []

    def get_agent_ids(self) -> Set[AgentID]:
        """Return the agent ids for the sub_environment.

        Returns:
            All agent ids for each the environment.
        """
        return {}

    def try_render(self, env_id: Optional[EnvID] = None) -> None:
        """Tries to render the sub-environment with the given id or all.

        Args:
            env_id: The sub-environment's ID, if applicable.
                If None, renders the entire Env (i.e. all sub-environments).
        """

        # By default, do nothing.
        pass

    def stop(self) -> None:
        """Releases all resources used."""

        # Try calling `close` on all sub-environments.
        for env in self.get_sub_environments():
            if hasattr(env, "close"):
                env.close()

    @property
    def observation_space(self) -> gym.Space:
        """Returns the observation space for each agent.

        Note: samples from the observation space need to be preprocessed into a
            `MultiEnvDict` before being used by a policy.

        Returns:
            The observation space for each environment.
        """
        raise NotImplementedError

    @property
    def action_space(self) -> gym.Space:
        """Returns the action space for each agent.

        Note: samples from the action space need to be preprocessed into a
            `MultiEnvDict` before being passed to `send_actions`.

        Returns:
            The observation space for each environment.
        """
        raise NotImplementedError

    def action_space_sample(self, agent_id: list = None) -> MultiEnvDict:
        """Returns a random action for each environment, and potentially each
            agent in that environment.

        Args:
            agent_id: List of agent ids to sample actions for. If None or empty
                list, sample actions for all agents in the environment.

        Returns:
            A random action for each environment.
        """
        logger.warning("action_space_sample() has not been implemented")
        del agent_id
        return {}

    def observation_space_sample(self, agent_id: list = None) -> MultiEnvDict:
        """Returns a random observation for each environment, and potentially
            each agent in that environment.

        Args:
            agent_id: List of agent ids to sample actions for. If None or empty
                list, sample actions for all agents in the environment.

        Returns:
            A random action for each environment.
        """
        logger.warning("observation_space_sample() has not been implemented")
        del agent_id
        return {}

    def last(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        """Returns the last observations, rewards, done- truncated flags and infos ...

        that were returned by the environment.

        Returns:
            The last observations, rewards, done- and truncated flags, and infos
            for each sub-environment.
        """
        logger.warning("last has not been implemented for this environment.")
        return {}, {}, {}, {}, {}

    def observation_space_contains(self, x: MultiEnvDict) -> bool:
        """Checks if the given observation is valid for each environment.

        Args:
            x: Observations to check.

        Returns:
            True if the observations are contained within their respective
                spaces. False otherwise.
        """
        return self._space_contains(self.observation_space, x)

    def action_space_contains(self, x: MultiEnvDict) -> bool:
        """Checks if the given actions is valid for each environment.

        Args:
            x: Actions to check.

        Returns:
            True if the actions are contained within their respective
                spaces. False otherwise.
        """
        return self._space_contains(self.action_space, x)

    def _space_contains(self, space: gym.Space, x: MultiEnvDict) -> bool:
        """Check if the given space contains the observations of x.

        Args:
            space: The space to if x's observations are contained in.
            x: The observations to check.

        Returns:
            True if the observations of x are contained in space.
        """
        agents = set(self.get_agent_ids())
        for multi_agent_dict in x.values():
            for agent_id, obs in multi_agent_dict.items():
                # this is for the case where we have a single agent
                # and we're checking a Vector env thats been converted to
                # a BaseEnv
                if agent_id == _DUMMY_AGENT_ID:
                    if not space.contains(obs):
                        return False
                # for the MultiAgent env case
                elif (agent_id not in agents) or (not space[agent_id].contains(obs)):
                    return False

        return True


# Fixed agent identifier when there is only the single agent in the env
_DUMMY_AGENT_ID = "agent0"


@OldAPIStack
def with_dummy_agent_id(
    env_id_to_values: Dict[EnvID, Any], dummy_id: "AgentID" = _DUMMY_AGENT_ID
) -> MultiEnvDict:
    ret = {}
    for (env_id, value) in env_id_to_values.items():
        # If the value (e.g. the observation) is an Exception, publish this error
        # under the env ID so the caller of `poll()` knows that the entire episode
        # (sub-environment) has crashed.
        ret[env_id] = value if isinstance(value, Exception) else {dummy_id: value}
    return ret


@OldAPIStack
def convert_to_base_env(
    env: EnvType,
    make_env: Callable[[int], EnvType] = None,
    num_envs: int = 1,
    remote_envs: bool = False,
    remote_env_batch_wait_ms: int = 0,
    worker: Optional["RolloutWorker"] = None,
    restart_failed_sub_environments: bool = False,
) -> "BaseEnv":
    """Converts an RLlib-supported env into a BaseEnv object.

    Supported types for the `env` arg are gym.Env, BaseEnv,
    VectorEnv, MultiAgentEnv, ExternalEnv, or ExternalMultiAgentEnv.

    The resulting BaseEnv is always vectorized (contains n
    sub-environments) to support batched forward passes, where n may also
    be 1. BaseEnv also supports async execution via the `poll` and
    `send_actions` methods and thus supports external simulators.

    TODO: Support gym3 environments, which are already vectorized.

    Args:
        env: An already existing environment of any supported env type
            to convert/wrap into a BaseEnv. Supported types are gym.Env,
            BaseEnv, VectorEnv, MultiAgentEnv, ExternalEnv, and
            ExternalMultiAgentEnv.
        make_env: A callable taking an int as input (which indicates the
            number of individual sub-environments within the final
            vectorized BaseEnv) and returning one individual
            sub-environment.
        num_envs: The number of sub-environments to create in the
            resulting (vectorized) BaseEnv. The already existing `env`
            will be one of the `num_envs`.
        remote_envs: Whether each sub-env should be a @ray.remote actor.
            You can set this behavior in your config via the
            `remote_worker_envs=True` option.
        remote_env_batch_wait_ms: The wait time (in ms) to poll remote
            sub-environments for, if applicable. Only used if
            `remote_envs` is True.
        worker: An optional RolloutWorker that owns the env. This is only
            used if `remote_worker_envs` is True in your config and the
            `on_sub_environment_created` custom callback needs to be called
            on each created actor.
        restart_failed_sub_environments: If True and any sub-environment (within
            a vectorized env) throws any error during env stepping, the
            Sampler will try to restart the faulty sub-environment. This is done
            without disturbing the other (still intact) sub-environment and without
            the RolloutWorker crashing.

    Returns:
        The resulting BaseEnv object.
    """

    from ray.rllib.env.remote_base_env import RemoteBaseEnv
    from ray.rllib.env.external_env import ExternalEnv
    from ray.rllib.env.multi_agent_env import MultiAgentEnv
    from ray.rllib.env.vector_env import VectorEnv, VectorEnvWrapper

    if remote_envs and num_envs == 1:
        raise ValueError(
            "Remote envs only make sense to use if num_envs > 1 "
            "(i.e. environment vectorization is enabled)."
        )

    # Given `env` has a `to_base_env` method -> Call that to convert to a BaseEnv type.
    if isinstance(env, (BaseEnv, MultiAgentEnv, VectorEnv, ExternalEnv)):
        return env.to_base_env(
            make_env=make_env,
            num_envs=num_envs,
            remote_envs=remote_envs,
            remote_env_batch_wait_ms=remote_env_batch_wait_ms,
            restart_failed_sub_environments=restart_failed_sub_environments,
        )
    # `env` is not a BaseEnv yet -> Need to convert/vectorize.
    else:
        # Sub-environments are ray.remote actors:
        if remote_envs:
            # Determine, whether the already existing sub-env (could
            # be a ray.actor) is multi-agent or not.
            multiagent = (
                ray.get(env._is_multi_agent.remote())
                if hasattr(env, "_is_multi_agent")
                else False
            )
            env = RemoteBaseEnv(
                make_env,
                num_envs,
                multiagent=multiagent,
                remote_env_batch_wait_ms=remote_env_batch_wait_ms,
                existing_envs=[env],
                worker=worker,
                restart_failed_sub_environments=restart_failed_sub_environments,
            )
        # Sub-environments are not ray.remote actors.
        else:
            # Convert gym.Env to VectorEnv ...
            env = VectorEnv.vectorize_gym_envs(
                make_env=make_env,
                existing_envs=[env],
                num_envs=num_envs,
                action_space=env.action_space,
                observation_space=env.observation_space,
                restart_failed_sub_environments=restart_failed_sub_environments,
            )
            # ... then the resulting VectorEnv to a BaseEnv.
            env = VectorEnvWrapper(env)

    # Make sure conversion went well.
    assert isinstance(env, BaseEnv), env

    return env
