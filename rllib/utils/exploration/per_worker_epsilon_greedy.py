from gym.spaces import Space
from typing import Optional

from ray.rllib.utils.exploration.epsilon_greedy import EpsilonGreedy
from ray.rllib.utils.schedules import ConstantSchedule


class PerWorkerEpsilonGreedy(EpsilonGreedy):
    """A per-worker epsilon-greedy class for distributed algorithms.

    Sets the epsilon schedules of individual workers to a constant:
    0.4 ^ (1 + [worker-index] / float([num-workers] - 1) * 7)
    See Ape-X paper.
    """

    def __init__(self, action_space: Space, *, framework: str,
                 num_workers: Optional[int], worker_index: Optional[int],
                 **kwargs):
        """Create a PerWorkerEpsilonGreedy exploration class.

        Args:
            action_space: The gym action space used by the environment.
            num_workers: The overall number of workers used.
            worker_index: The index of the Worker using this
                Exploration.
            framework: One of None, "tf", "torch".
        """
        epsilon_schedule = None
        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        assert worker_index <= num_workers, (worker_index, num_workers)
        if num_workers > 0:
            if worker_index > 0:
                # From page 5 of https://arxiv.org/pdf/1803.00933.pdf
                alpha, eps, i = 7, 0.4, worker_index - 1
                num_workers_minus_1 = float(num_workers - 1) \
                    if num_workers > 1 else 1.0
                constant_eps = eps**(1 + (i / num_workers_minus_1) * alpha)
                epsilon_schedule = ConstantSchedule(
                    constant_eps, framework=framework)
            # Local worker should have zero exploration so that eval
            # rollouts run properly.
            else:
                epsilon_schedule = ConstantSchedule(0.0, framework=framework)

        super().__init__(
            action_space,
            epsilon_schedule=epsilon_schedule,
            framework=framework,
            num_workers=num_workers,
            worker_index=worker_index,
            **kwargs)
