from ray.rllib.utils.schedules.schedule import Schedule
from ray.rllib.utils.framework import check_framework, try_import_tf

tf = try_import_tf()


class PolynomialSchedule(Schedule):

    def __init__(self, schedule_timesteps, final_p, initial_p=1.0,
                 power=2.0, framework=None):
        """
        Polynomial interpolation between initial_p and final_p over
        schedule_timesteps. After this many time steps always `final_p` is
        returned.

        Agrs:
            schedule_timesteps (int): Number of time steps for which to
                linearly anneal initial_p to final_p
            final_p (float): Final output value.
            initial_p (float): Initial output value.
            framework (Optional[str]): One of "tf", "torch", or None.
        """
        super().__init__()
        assert self.schedule_timesteps > 0
        self.schedule_timesteps = schedule_timesteps
        self.final_p = final_p
        self.initial_p = initial_p
        self.power = power
        self.framework = check_framework(framework)

    def value(self, t):
        """
        Returns the result of:
        final_p + (from_ - to_) * (1 - `t`/t_max) ** power
        """
        if self.framework == "tf":
            return tf.train.polynomial_decay(
                learning_rate=self.initial_p, global_step=t,
                decay_steps=self.schedule_timesteps,
                end_learning_rate=self.final_p,
                power=self.power
            )
        return self.final_p + (self.final_p - self.initial_p) * (
                1.0 - (t / self.schedule_timesteps)) ** self.power
