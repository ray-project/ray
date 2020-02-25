from ray.rllib.utils.exploration.gaussian_noise import GaussianNoise
from ray.rllib.utils.schedules import ConstantSchedule


class PerWorkerGaussianNoise(GaussianNoise):
    """A per-worker Gaussian noise class for distributed algorithms.

    Sets the `scale` schedules of individual workers to a constant:
    0.4 ^ (1 + [worker-index] / float([num-workers] - 1) * 7)
    See Ape-X paper.
    """

    def __init__(self,
                 action_space,
                 random_timesteps=1000,
                 stddev=0.1,
                 initial_scale=1.0,
                 final_scale=0.02,
                 scale_timesteps=10000,
                 num_workers=0,
                 worker_index=0,
                 framework="tf"):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            random_timesteps (int): The number of timesteps for which to act
                completely randomly. Only after this number of timesteps, the
                `self.scale` annealing process will start (see below).
            stddev (float): The stddev (sigma) to use for the
                Gaussian noise to be added to the actions.
            initial_scale (float): The initial scaling weight to multiply
                the noise with.
            final_scale (float): The final scaling weight to multiply
                the noise with.
            scale_timesteps (int): The timesteps over which to linearly anneal
                the scaling factor (after(!) having used random actions for
                `random_timesteps` steps.
            num_workers (Optional[int]): The overall number of workers used.
            worker_index (Optional[int]): The index of the Worker using this
                Exploration.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        scale_schedule = None
        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        if num_workers > 0:
            if worker_index >= 0:
                exponent = (1 + worker_index / float(num_workers - 1) * 7)
                scale_schedule = ConstantSchedule(0.4**exponent)
            # Local worker should have zero exploration so that eval
            # rollouts run properly.
            else:
                scale_schedule = ConstantSchedule(0.0)

        super().__init__(
            action_space=action_space,
            random_timesteps=random_timesteps,
            stddev=stddev,
            initial_scale=initial_scale,
            final_scale=final_scale,
            scale_timesteps=scale_timesteps,
            framework=framework,
            scale_schedule=scale_schedule)
