from collections import defaultdict

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.schedules.piecewise_schedule import PiecewiseSchedule


_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class Scheduler:
    def __init__(self, fixed_value, schedule, framework):

        self.use_schedule = schedule is not None
        self.framework = framework

        if self.use_schedule:
            # Custom schedule, based on list of
            # ([ts], [value to be reached by ts])-tuples.
            self.entropy_coeff_schedule_per_module = defaultdict(
                lambda: PiecewiseSchedule(
                    schedule,
                    outside_value=schedule[-1][-1],
                    framework=None,
                )
            )
            # As initial entropy coeff value, use the first timestep's (must be 0)
            # value.
            self.curr_entropy_coeffs_per_module = defaultdict(
                lambda: self._get_tensor_variable(
                    schedule[0][1],
                    framework=self.framework,
                )
            )
        # If no schedule, pin entropy coeff to its given (fixed) value.
        else:
            self.curr_entropy_coeffs_per_module = defaultdict(lambda: fixed_value)

    @staticmethod
    def validate(schedule, schedule_name, value_name):
        # Schedule checking: Any schedule data must start at ts=0 to avoid
        # ambiguity (user might think that the fixed setting (e.g. `lr` vs
        # `lr_schedule`) plays a role as well of that that's the initial value,
        # when it isn't). If a schedule is provided (not None), we ignore the
        # corresponding fixed value.
        if schedule is not None:
            if not isinstance(schedule, (list, tuple)) or (len(schedule) < 2):
                raise ValueError(
                    f"Invalid `{schedule_name}` ({schedule}) specified! Must be a "
                    "list of at least 2 tuples, each of the form "
                    f"(`timestep`, `{value_name} to reach`), e.g. "
                    "`[(0, 0.001), (1e6, 0.0001), (2e6, 0.00005)]`."
                )
            elif schedule[0][0] != 0:
                raise ValueError(
                    f"When providing a `{schedule_name}`, the first timestep must be 0 "
                    f"and the corresponding lr value is the initial {value_name}! You "
                    f"provided ts={schedule[0][0]} {value_name}={schedule[0][1]}."
                )

    def get_current_value(self, module_id):
        return self.curr_entropy_coeffs_per_module[module_id]

    def update(self, module_id, timestep: int) -> float:
        if self.use_schedule:
            value = self.entropy_coeff_schedule_per_module[module_id].value(t=timestep)
            if self.framework == "torch":
                self.curr_entropy_coeffs_per_module[module_id].data = torch.tensor(
                    value
                )
            else:
                self.curr_entropy_coeffs_per_module[module_id].assign(value)
        else:
            value = self.curr_entropy_coeffs_per_module[module_id]

        return value

    def _get_tensor_variable(self, initial_value, framework, device=None):
        if framework == "torch":
            return torch.tensor(
                initial_value,
                requires_grad=False,
                dtype=torch.float32,
                device=device,
            )
        else:
            return tf.Variable(
                initial_value,
                trainable=False,
                dtype=tf.float32,
            )
