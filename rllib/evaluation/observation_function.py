from typing import Dict

from ray.rllib.env import BaseEnv
from ray.rllib.policy import Policy, AgentID, PolicyID
from ray.rllib.evaluation import MultiAgentEpisode, RolloutWorker
from ray.rllib.utils.framework import TensorType


class ObservationFunction:
    """Interceptor function for rewriting observations from the environment.

    These callbacks can be used for preprocessing of observations, especially
    in multi-agent scenarios.

    Observations functions can be specified in the multi-agent config by
    specifying ``{"observation_function": your_obs_func}``. Note that
    ``your_obs_func`` can be a plain Python function.

    This API is **experimental**.
    """

    def __call__(self, agent_obs: Dict[AgentID, TensorType],
                 worker: RolloutWorker, base_env: BaseEnv,
                 policies: Dict[PolicyID, Policy], episode: MultiAgentEpisode,
                 **kw) -> Dict[AgentID, TensorType]:
        """Callback run on each environment step to observe the environment.

        This method takes in the original agent observation dict returned by
        a MultiAgentEnv, and returns a possibly modified one. It can be
        thought of as a "wrapper" around the environment.

        TODO(ekl): allow end-to-end differentiation through the observation
            function and policy losses.

        TODO(ekl): enable batch processing.

        Args:
            agent_obs (dict): Dictionary of default observations from the
                environment. The default implementation of observe() simply
                returns this dict.
            worker (RolloutWorker): Reference to the current rollout worker.
            base_env (BaseEnv): BaseEnv running the episode. The underlying
                env object can be gotten by calling base_env.get_unwrapped().
            policies (dict): Mapping of policy id to policy objects. In single
                agent mode there will only be a single "default" policy.
            episode (MultiAgentEpisode): Episode state object.
            kwargs: Forward compatibility placeholder.

        Returns:
            new_agent_obs (dict): copy of agent obs with updates. You can
                rewrite or drop data from the dict if needed (e.g., the env
                can have a dummy "global" observation, and the observer can
                merge the global state into individual observations.

        Examples:
            >>> # Observer that merges global state into individual obs. It is
            ... # rewriting the discrete obs into a tuple with global state.
            >>> example_obs_fn1({"a": 1, "b": 2, "global_state": 101}, ...)
            {"a": [1, 101], "b": [2, 101]}

            >>> # Observer for e.g., custom centralized critic model. It is
            ... # rewriting the discrete obs into a dict with more data.
            >>> example_obs_fn2({"a": 1, "b": 2}, ...)
            {"a": {"self": 1, "other": 2}, "b": {"self": 2, "other": 1}}
        """

        return agent_obs
