from typing import Optional

from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.schedules.schedule import Schedule
from ray.rllib.utils.typing import TensorType

torch, _ = try_import_torch()


class ExponentialSchedule(Schedule):
    """Exponential decay schedule from `initial_p` to `final_p`.

    Reduces output over `schedule_timesteps`. After this many time steps
    always returns `final_p`.
    """

    def __init__(self,
                 schedule_timesteps: int,
                 framework: Optional[str] = None,
                 initial_p: float = 1.0,
                 decay_rate: float = 0.1):
        """Initializes a ExponentialSchedule instance.

        Args:
            schedule_timesteps: Number of time steps for which to
                linearly anneal initial_p to final_p.
            framework: The framework descriptor string, e.g. "tf",
                "torch", or None.
            initial_p: Initial output value.
            decay_rate: The percentage of the original value after
                100% of the time has been reached (see formula above).
                >0.0: The smaller the decay-rate, the stronger the decay.
                1.0: No decay at all.
        """
        super().__init__(framework=framework)
        assert schedule_timesteps > 0
        self.schedule_timesteps = schedule_timesteps
        self.initial_p = initial_p
        self.decay_rate = decay_rate

    @override(Schedule)
    def _value(self, t: TensorType) -> TensorType:
        """Returns the result of: initial_p * decay_rate ** (`t`/t_max).
        """
        if self.framework == "torch" and torch and isinstance(t, torch.Tensor):
            t = t.float()
        return self.initial_p * \
            self.decay_rate ** (t / self.schedule_timesteps)
