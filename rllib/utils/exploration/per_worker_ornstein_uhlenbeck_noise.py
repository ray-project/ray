from gym.spaces import Space
from typing import Optional

from ray.rllib.utils.exploration.ornstein_uhlenbeck_noise import \
    OrnsteinUhlenbeckNoise
from ray.rllib.utils.schedules import ConstantSchedule


class PerWorkerOrnsteinUhlenbeckNoise(OrnsteinUhlenbeckNoise):
    """A per-worker Ornstein Uhlenbeck noise class for distributed algorithms.

    Sets the Gaussian `scale` schedules of individual workers to a constant:
    0.4 ^ (1 + [worker-index] / float([num-workers] - 1) * 7)
    See Ape-X paper.
    """

    def __init__(self, action_space: Space, *, framework: Optional[str],
                 num_workers: Optional[int], worker_index: Optional[int],
                 **kwargs):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            num_workers (Optional[int]): The overall number of workers used.
            worker_index (Optional[int]): The index of the Worker using this
                Exploration.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        scale_schedule = None
        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        if num_workers > 0:
            if worker_index > 0:
                num_workers_minus_1 = float(num_workers - 1) \
                    if num_workers > 1 else 1.0
                exponent = (1 + (worker_index / num_workers_minus_1) * 7)
                scale_schedule = ConstantSchedule(
                    0.4**exponent, framework=framework)
            # Local worker should have zero exploration so that eval
            # rollouts run properly.
            else:
                scale_schedule = ConstantSchedule(0.0, framework=framework)

        super().__init__(
            action_space,
            scale_schedule=scale_schedule,
            num_workers=num_workers,
            worker_index=worker_index,
            framework=framework,
            **kwargs)
