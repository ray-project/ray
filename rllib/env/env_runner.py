import abc
import platform
from typing import Optional, Union

import ray
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class EnvRunner(FaultAwareApply, metaclass=abc.ABCMeta):
    """Base class for RLlib data collection from a gymnasium.vector.Env.

    Sub-class `EnvRunner` for custom behavior and specify your custom class
    via the AlgorithmConfig.rollouts(env_runner_cls=...) setting.

    TODO:
    This class wraps a Model and a gymnasium.vector.Env instance for
    collecting data from an environment. Users can create many replicas of
    this class as Ray actors to scale RL training.

    TODO:
    Examples:
        >>> # Create a rollout worker and using it to collect experiences.
        >>> import gymnasium as gym
        >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
        >>> from ray.rllib.algorithms.pg.pg_tf_policy import PGTF1Policy
        >>> worker = RolloutWorker( # doctest: +SKIP
        ...   env_creator=lambda _: gym.make("CartPole-v1"), # doctest: +SKIP
        ...   default_policy_class=PGTF1Policy) # doctest: +SKIP
        >>> print(worker.sample()) # doctest: +SKIP
        SampleBatch({
            "obs": [[...]], "actions": [[...]], "rewards": [[...]],
            "terminateds": [[...]], "truncateds": [[...]], "new_obs": [[...]]})
        >>> # Creating a multi-agent rollout worker
        >>> from gymnasium.spaces import Discrete, Box
        >>> import random
        >>> MultiAgentTrafficGrid = ... # doctest: +SKIP
        >>> worker = RolloutWorker( # doctest: +SKIP
        ...   env_creator=lambda _: MultiAgentTrafficGrid(num_cars=25),
        ...   config=AlgorithmConfig().multi_agent(
        ...     policies={ # doctest: +SKIP
        ...       # Use an ensemble of two policies for car agents
        ...       "car_policy1": # doctest: +SKIP
        ...         (PGTFPolicy, Box(...), Discrete(...),
        ...          AlgorithmConfig.overrides(gamma=0.99)),
        ...       "car_policy2": # doctest: +SKIP
        ...         (PGTFPolicy, Box(...), Discrete(...),
        ...          AlgorithmConfig.overrides(gamma=0.95)),
        ...       # Use a single shared policy for all traffic lights
        ...       "traffic_light_policy":
        ...         (PGTFPolicy, Box(...), Discrete(...), {}),
        ...     },
        ...     policy_mapping_fn=(
        ...       lambda agent_id, episode, **kwargs:
        ...       random.choice(["car_policy1", "car_policy2"])
        ...       if agent_id.startswith("car_") else "traffic_light_policy"),
        ...     ),
        ..  )
        >>> print(worker.sample()) # doctest: +SKIP
        MultiAgentBatch({
            "car_policy1": SampleBatch(...),
            "car_policy2": SampleBatch(...),
            "traffic_light_policy": SampleBatch(...)})
    """

    def __init__(self, config):
        self.config = config

    @classmethod
    @DeveloperAPI
    def as_remote(
        cls,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[Union[int, float]] = None,
        memory: Optional[int] = None,
        resources: Optional[dict] = None,
        max_num_worker_restarts: int = 0,
    ) -> type:
        """Returns RolloutWorker class as a `@ray.remote using given options`.

        The returned class can then be used to instantiate ray actors.

        Args:
            num_cpus: The number of CPUs to allocate for the remote actor.
            num_gpus: The number of GPUs to allocate for the remote actor.
                This could be a fraction as well.
            memory: The heap memory request in bytes for this task/actor,
                rounded down to the nearest integer.
            resources: The default custom resources to allocate for the remote
                actor.

        Returns:
            The `@ray.remote` decorated RolloutWorker class.
        """
        return ray.remote(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            resources=resources,
            # Automatically restart failed workers.
            max_restarts=max_num_worker_restarts,
        )(cls)

    @abc.abstractmethod
    def assert_healthy(self):
        """Checks that __init__ has been completed properly.

        Useful in case an `EnvRunner` is run as @ray.remote (Actor) and the owner
        would like to make sure the Ray Actor has been properly initialized.

        Raises:
            AssertionError: If the worker has NOT been properly initialized.
        """

    @abc.abstractmethod
    def sample(self):
        """Returns a batch of experience sampled from this EnvRunner.

        This method must be implemented by subclasses.

        Returns:
            A columnar batch of experiences (e.g., tensors).

        TODO:
        Examples:
            >>> import gymnasium as gym
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> from ray.rllib.algorithms.pg.pg_tf_policy import PGTF1Policy
            >>> worker = RolloutWorker( # doctest: +SKIP
            ...   env_creator=lambda _: gym.make("CartPole-v1"), # doctest: +SKIP
            ...   default_policy_class=PGTF1Policy, # doctest: +SKIP
            ...   config=AlgorithmConfig(), # doctest: +SKIP
            ... )
            >>> print(worker.sample()) # doctest: +SKIP
            SampleBatch({"obs": [...], "action": [...], ...})
        """

    @abc.abstractmethod
    def get_metrics(self):
        """Returns the thus-far collected metrics from this worker's rollouts.

        Returns:
             List of RolloutMetrics
             collected thus-far.
        """

    def get_host(self) -> str:
        """Returns the hostname of the process running this evaluator."""
        return platform.node()

    def get_node_ip(self) -> str:
        """Returns the IP address of the node that this worker runs on."""
        return ray.util.get_node_ip_address()

    def find_free_port(self) -> int:
        """Finds a free port on the node that this worker runs on."""
        from ray.air._internal.util import find_free_port

        return find_free_port()

    @abc.abstractmethod
    def __del__(self):
        """If this Actor is deleted, clears all resources used by it."""
