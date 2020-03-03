from ray.rllib.utils.exploration.epsilon_greedy import EpsilonGreedy
from ray.rllib.utils.schedules import ConstantSchedule


class PerWorkerEpsilonGreedy(EpsilonGreedy):
    """A per-worker epsilon-greedy class for distributed algorithms.

    Sets the epsilon schedules of individual workers to a constant:
    0.4 ^ (1 + [worker-index] / float([num-workers] - 1) * 7)
    See Ape-X paper.
    """

    def __init__(self,
                 action_space,
                 *,
                 num_workers=0,
                 worker_index=0,
                 framework="tf",
                 **kwargs):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            num_workers (Optional[int]): The overall number of workers used.
            worker_index (Optional[int]): The index of the Worker using this
                Exploration.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        epsilon_schedule = None
        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        if num_workers > 0:
            if worker_index >= 0:
                exponent = (1 + worker_index / float(num_workers - 1) * 7)
                epsilon_schedule = ConstantSchedule(0.4**exponent)
            # Local worker should have zero exploration so that eval
            # rollouts run properly.
            else:
                epsilon_schedule = ConstantSchedule(0.0)

        super().__init__(
            action_space,
            epsilon_schedule=epsilon_schedule,
            framework=framework,
            **kwargs)
