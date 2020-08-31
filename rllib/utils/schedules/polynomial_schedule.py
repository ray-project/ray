from typing import Union

from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.schedules.schedule import Schedule
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class PolynomialSchedule(Schedule):
    def __init__(self,
                 schedule_timesteps,
                 final_p,
                 framework,
                 initial_p=1.0,
                 power=2.0):
        """
        Polynomial interpolation between initial_p and final_p over
        schedule_timesteps. After this many time steps, always `final_p` is
        returned.

        Agrs:
            schedule_timesteps (int): Number of time steps for which to
                linearly anneal initial_p to final_p
            final_p (float): Final output value.
            initial_p (float): Initial output value.
            framework (Optional[str]): One of "tf", "torch", or None.
        """
        super().__init__(framework=framework)
        assert schedule_timesteps > 0
        self.schedule_timesteps = schedule_timesteps
        self.final_p = final_p
        self.initial_p = initial_p
        self.power = power

    @override(Schedule)
    def _value(self, t: Union[int, TensorType]):
        """Returns the result of:
        final_p + (initial_p - final_p) * (1 - `t`/t_max) ** power
        """
        if self.framework == "torch" and torch and isinstance(t, torch.Tensor):
            t = t.float()
        t = min(t, self.schedule_timesteps)
        return self.final_p + (self.initial_p - self.final_p) * (
            1.0 - (t / self.schedule_timesteps))**self.power

    @override(Schedule)
    def _tf_value_op(self, t: Union[int, TensorType]):
        t = tf.math.minimum(t, self.schedule_timesteps)
        return self.final_p + (self.initial_p - self.final_p) * (
            1.0 - (t / self.schedule_timesteps))**self.power
