from gym.spaces import Space
from typing import Optional

from ray.rllib.utils.annotations import override
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
                 alpha: float = 7,
                 eps: float = 0.4,
                 **kwargs):
        """Create a PerWorkerEpsilonGreedy exploration class.

        Args:
            action_space (Space): The gym action space used by the environment.
            num_workers (Optional[int]): The overall number of workers used.
            worker_index (Optional[int]): The index of the Worker using this
                Exploration.
            framework (Optional[str]): One of None, "tf", "torch".
            alpha (int): multiplicative factor of the exponent used
            to compute the epsilon. The smaller the amount of rollout-workers,
            the smaller the exponent should be. This helps to avoid too low
            exploration rates. The default value here is took from
            page 6 of https://arxiv.org/pdf/1803.00933.pdf
            eps (float): base value use to compute the epsilon to be used
            by a worker. The default value here is took from
            page 6 of https://arxiv.org/pdf/1803.00933.pdf
        """
        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        assert worker_index <= num_workers, (worker_index, num_workers)
        self.num_workers = num_workers
        self.worker_index = worker_index
        self.alpha = alpha
        self.eps = eps

        super().__init__(
            action_space,
            framework=framework,
            num_workers=num_workers,
            worker_index=worker_index,
            **kwargs)

    @override(EpsilonGreedy)
    def reset_schedule(self,
                       initial_epsilon=None,
                       final_epsilon=None,
                       epsilon_timesteps=None,
                       epsilon_schedule=None
                       ):
        epsilon_schedule = None
        if self.num_workers > 0:
            if self.worker_index > 0:
                # The values used here, by default (see constructor defaults),
                # are the same epsilon and alpha of the e-greedy policy use in
                # page 6 of https://arxiv.org/pdf/1803.00933.pdf
                i = self.worker_index - 1
                num_workers_minus_1 = float(self.num_workers - 1) \
                    if self.num_workers > 1 else 1.0
                epsilon_schedule = ConstantSchedule(
                    self.eps**(1 + (i / num_workers_minus_1) * self.alpha),
                    framework=self.framework)
            # Local worker should have zero exploration so that eval
            # rollouts run properly.
            else:
                epsilon_schedule = ConstantSchedule(0.0, framework=self.framework)

        super().reset_schedule(initial_epsilon=initial_epsilon,
                       final_epsilon=final_epsilon,
                       epsilon_timesteps=epsilon_timesteps,
                       epsilon_schedule=epsilon_schedule)
