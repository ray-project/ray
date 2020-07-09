from ray.rllib.utils.exploration.gaussian_noise import GaussianNoise
from ray.rllib.utils.schedules import ConstantSchedule


class PerWorkerGaussianNoise(GaussianNoise):
    """A per-worker Gaussian noise class for distributed algorithms.

    Sets the `scale` schedules of individual workers to a constant:
    0.4 ^ (1 + [worker-index] / float([num-workers] - 1) * 7)
    See Ape-X paper.
    """

    def __init__(self, action_space, *, framework, num_workers, worker_index,
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
                exponent = (1 + worker_index / float(num_workers - 1) * 7)
                scale_schedule = ConstantSchedule(
                    0.4**exponent, framework=framework)
            # Local worker should have zero exploration so that eval
            # rollouts run properly.
            else:
                scale_schedule = ConstantSchedule(0.0, framework=framework)

        super().__init__(
            action_space,
            scale_schedule=scale_schedule,
            framework=framework,
            **kwargs)
