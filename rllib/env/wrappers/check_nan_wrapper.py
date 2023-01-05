import logging
import numpy as np
from typing import Optional, Tuple

from ray.rllib.env.base_env import BaseEnv
from ray.rllib.utils.spaces.space_utils import flatten_to_single_ndarray
from ray.rllib.utils.typing import EnvID, MultiEnvDict

logger = logging.getLogger(__name__)


class CheckNaNWrapper:
    def __init__(
        self,
        env: BaseEnv,
        config: dict,
    ):
        # Only BaseEnvs should be wrapped.
        assert isinstance(env, BaseEnv)
        self.env = env

        # Whether the wrapper should raise an exception when finding
        # an NaN or Inf value.
        self.raise_exception: bool = config.get("raise_exception", False)
        # Whether it should be checked also for Inf values. Otherwise only NaN
        # values are checked for.
        self.check_inf: bool = config.get("check_inf", True)
        # Whether the user should be warned only once, if an NaN or Inf value
        # was encountered.
        self.warn_once: bool = config.get("warn_once", False)
        self._user_warned: bool = False

        # Records also the last action and observation for understanding the causes.
        self._last_actions: MultiEnvDict = None
        self._last_observations: MultiEnvDict = None
        self._reset = True

    def __getattr__(self, name):
        """Uses all attributes of the wrapped environment.

        This ensures that all attributes of the BaseEnv are available
        upon calling.

        Args:
            name: Name of the attribute.

        Returns:
            Class attribute of the wrapped BaseEnv.
        """
        if name.startswith("_"):
            raise AttributeError(f"accessing private attribute '{name}' is prohibited")
        return getattr(self.env, name)

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
        """Checks for Nan and Inf values in observations and rewards.

        The `poll()` method of the BaseEnv is wrapped here.

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
        (
            unfiltered_obs,
            rewards,
            terminateds,
            truncateds,
            infos,
            off_policy_actions,
        ) = self.env.poll()

        if isinstance(unfiltered_obs, dict):
            self._last_observations = unfiltered_obs

            # Check observations.
            for env_id, all_agent_obs in unfiltered_obs.items():
                if not isinstance(all_agent_obs, Exception):
                    for agent_id, agent_obs in all_agent_obs.items():
                        self._check_val(
                            env_id=env_id,
                            agent_id=agent_id,
                            observation=agent_obs,
                        )
                else:
                    return (
                        unfiltered_obs,
                        rewards,
                        terminateds,
                        truncateds,
                        infos,
                        off_policy_actions,
                    )
            # Check rewards.
            for env_id, all_agent_rewards in rewards.items():
                if not isinstance(all_agent_rewards, Exception):
                    for agent_id, agent_reward in all_agent_rewards.items():
                        self._check_val(
                            env_id=env_id,
                            agent_id=agent_id,
                            reward=agent_reward,
                        )
                else:
                    return (
                        unfiltered_obs,
                        rewards,
                        terminateds,
                        truncateds,
                        infos,
                        off_policy_actions,
                    )

        # Finally, return the polled results from the BaseEnv back to the
        # sampler.
        return (
            unfiltered_obs,
            rewards,
            terminateds,
            truncateds,
            infos,
            off_policy_actions,
        )

    def send_actions(self, action_dict: MultiEnvDict) -> None:
        """Checking for NaNs and Inf in actions.

        The `send_actions()` of the BaseEnv is wrapped here.

        Args:
            action_dict: Actions values keyed by env_id and agent_id.
        """
        if isinstance(action_dict, dict):
            self._last_actions = action_dict

            # Check actions.
            for env_id, all_agent_actions in action_dict.items():
                for agent_id, agent_actions in all_agent_actions.items():
                    self._check_val(
                        env_id=env_id,
                        agent_id=agent_id,
                        action=agent_actions,
                    )

        # Finally return the actions to the wrapped BaseEnv for evaluation.
        return self.env.send_actions(action_dict)

    def try_reset(
        self,
        env_id: Optional[EnvID] = None,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[MultiEnvDict, MultiEnvDict]:
        """Checks values from the environment at reset.

        The `try_reset()` of the BaseEnv is wrapped here.

        Args:
            env_id: The sub-environment's ID if applicable. If None, reset
                the entire Env (i.e. all sub-environments).
            seed: The seed to be passed to the sub-environment(s) when
                resetting it. If None, will not reset any existing PRNG. If you pass an
                integer, the PRNG will be reset even if it already exists.
            options: An options dict to be passed to the sub-environment(s) when
                resetting it.

        Returns:
            The wrapped environment's reset observations and infos.
        """
        resetted_obs, resetted_infos = self.env.try_reset(
            env_id=env_id,
            seed=seed,
            options=options,
        )

        self._last_observations = resetted_obs

        if isinstance(resetted_obs, dict):
            # Check observations.
            for env_id, all_agent_obs in resetted_obs.items():
                if not isinstance(all_agent_obs, Exception):
                    for agent_id, agent_obs in all_agent_obs.items():
                        self._check_val(
                            env_id=env_id,
                            agent_id=agent_id,
                            observation=agent_obs,
                        )
                else:
                    return resetted_obs, resetted_infos

        # Reset is done.
        self._reset = False

        # Finally return the BaseEnv's resetted observations and infos.
        return resetted_obs, resetted_infos

    def _check_val(self, env_id, agent_id, **kwargs) -> None:
        """Checks values from the environment for NaN or Inf.

        Values get assigned to their corresponding names and a
        message is printed that includes also the last intact action.

        Args:
            env_id: The sub-environment's ID, if applicable.
            agent_id: The agent's ID, if applicable.

        Returns:
            A message containing information about the detected NaN/Inf values
            or None, if all values are proper numbers.
        """

        # Only execute value checking if False.
        if not self.raise_exception and self.warn_once and self._user_warned:
            return

        found = []
        for name, val in kwargs.items():
            has_nan = np.any(np.isnan(flatten_to_single_ndarray(val)))
            has_inf = self.check_inf and np.any(
                np.isinf(flatten_to_single_ndarray(val))
            )
            if has_inf:
                found.append((name, "inf"))
            if has_nan:
                found.append((name, "nan"))

        if found:
            self._user_warned = True
            msg = (
                ""
                if len(found) == 0
                else "Sub-Env '{}', agent '{}': ".format(env_id, agent_id)
            )
            for i, (name, val) in enumerate(found):
                msg += "found '{}' in '{}'".format(val, name)
                if i != len(found) - 1:
                    msg += ", "

            msg += ".\r\nOriginated from the "
            if self._reset:
                # At reset no last action was recorded.
                msg += "environment (at reset). "
                msg += "Environment returned: \r\n'observation'={}.".format(
                    self._last_observations[env_id]
                )
            elif found[0][0] == "observation":
                msg += "environment. Last given value was: \r\n'action'={}.".format(
                    self._last_actions[env_id]
                )
                msg += "\r\nStep result was: \r\n'observation'={}".format(
                    self._last_observations[env_id]
                )
            else:
                msg += (
                    "Policy model. Last given value was: \r\n'observation'={}.".format(
                        self._last_observations[env_id]
                    )
                )
                msg += "\r\nResult was: \r\n'action'={}".format(
                    self._last_action[env_id]
                )

            if self.raise_exception:
                raise ValueError(msg)
            else:
                logger.warning(msg)
