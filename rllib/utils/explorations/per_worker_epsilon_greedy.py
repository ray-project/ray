from ray.rllib.utils.explorations.epsilon_greedy import EpsilonGreedy
from ray.rllib.utils.schedules import ConstantSchedule


class PerWorkerEpsilonGreedy(EpsilonGreedy):
    """
    An epsilon-greedy Exploration class that produces exploration actions
    when given a Model's output and a current epsilon value (based on some
    Schedule).
    """

    def __init__(self,
                 action_space,
                 initial_epsilon=1.0,
                 final_epsilon=0.1,
                 epsilon_timesteps=int(1e5),
                 worker_info=None,
                 framework="tf"):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            initial_epsilon (float): The initial epsilon value to use.
            final_epsilon (float): The final epsilon value to use.
            epsilon_timesteps (int): The time step after which epsilon should
                always be `final_epsilon`.
            worker_info (Optional[Dict[str,any]]): Dict with keys:
                `num_workers`: The overall number of workers used.
                `worker_index`: The index of the Worker using this Exploration.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        epsilon_schedule = None
        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        idx = self.worker_info.get("worker_index", 0)
        num = self.worker_info.get("num_workers", 0)
        if num > 0:
            if idx >= 0:
                exponent = (1 + idx / float(num - 1) * 7)
                epsilon_schedule = ConstantSchedule(0.4**exponent)
            # Local worker should have zero exploration so that eval
            # rollouts run properly.
            else:
                epsilon_schedule = ConstantSchedule(0.0)

        super().__init__(
            action_space=action_space,
            initial_epsilon=initial_epsilon,
            final_epsilon=final_epsilon,
            epsilon_timesteps=epsilon_timesteps,
            worker_info=worker_info,
            framework=framework,
            epsilon_schedule=epsilon_schedule)
