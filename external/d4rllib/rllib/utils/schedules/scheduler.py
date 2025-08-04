from typing import Optional

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.schedules.piecewise_schedule import PiecewiseSchedule
from ray.rllib.utils.typing import LearningRateOrSchedule, TensorType
from ray.util.annotations import DeveloperAPI


_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


@DeveloperAPI
class Scheduler:
    """Class to manage a scheduled (framework-dependent) tensor variable.

    Uses the PiecewiseSchedule (for maximum configuration flexibility)
    """

    def __init__(
        self,
        fixed_value_or_schedule: LearningRateOrSchedule,
        *,
        framework: str = "torch",
        device: Optional[str] = None,
    ):
        """Initializes a Scheduler instance.

        Args:
            fixed_value_or_schedule: A fixed, constant value (in case no schedule should
                be used) or a schedule configuration in the format of
                [[timestep, value], [timestep, value], ...]
                Intermediary timesteps will be assigned to linerarly interpolated
                values. A schedule config's first entry must
                start with timestep 0, i.e.: [[0, initial_value], [...]].
            framework: The framework string, for which to create the tensor variable
                that hold the current value. This is the variable that can be used in
                the graph, e.g. in a loss function.
            device: Optional device (for torch) to place the tensor variable on.
        """
        self.framework = framework
        self.device = device
        self.use_schedule = isinstance(fixed_value_or_schedule, (list, tuple))

        if self.use_schedule:
            # Custom schedule, based on list of
            # ([ts], [value to be reached by ts])-tuples.
            self._schedule = PiecewiseSchedule(
                fixed_value_or_schedule,
                outside_value=fixed_value_or_schedule[-1][-1],
                framework=None,
            )
            # As initial tensor valie, use the first timestep's (must be 0) value.
            self._curr_value = self._create_tensor_variable(
                initial_value=fixed_value_or_schedule[0][1]
            )

        # If no schedule, pin (fix) given value.
        else:
            self._curr_value = fixed_value_or_schedule

    @staticmethod
    def validate(
        *,
        fixed_value_or_schedule: LearningRateOrSchedule,
        setting_name: str,
        description: str,
    ) -> None:
        """Performs checking of a certain schedule configuration.

        The first entry in `value_or_schedule` (if it's not a fixed value) must have a
        timestep of 0.

        Args:
            fixed_value_or_schedule: A fixed, constant value (in case no schedule should
                be used) or a schedule configuration in the format of
                [[timestep, value], [timestep, value], ...]
                Intermediary timesteps will be assigned to linerarly interpolated
                values. A schedule config's first entry must
                start with timestep 0, i.e.: [[0, initial_value], [...]].
            setting_name: The property name of the schedule setting (within a config),
                e.g. `lr` or `entropy_coeff`.
            description: A full text description of the property that's being scheduled,
                e.g. `learning rate`.

        Raises:
            ValueError: In case, errors are found in the schedule's format.
        """
        # Fixed (single) value.
        if (
            isinstance(fixed_value_or_schedule, (int, float))
            or fixed_value_or_schedule is None
        ):
            return

        if not isinstance(fixed_value_or_schedule, (list, tuple)) or (
            len(fixed_value_or_schedule) < 2
        ):
            raise ValueError(
                f"Invalid `{setting_name}` ({fixed_value_or_schedule}) specified! "
                f"Must be a list of 2 or more tuples, each of the form "
                f"(`timestep`, `{description} to reach`), for example "
                "`[(0, 0.001), (1e6, 0.0001), (2e6, 0.00005)]`."
            )
        elif fixed_value_or_schedule[0][0] != 0:
            raise ValueError(
                f"When providing a `{setting_name}` schedule, the first timestep must "
                f"be 0 and the corresponding lr value is the initial {description}! "
                f"You provided ts={fixed_value_or_schedule[0][0]} {description}="
                f"{fixed_value_or_schedule[0][1]}."
            )
        elif any(len(pair) != 2 for pair in fixed_value_or_schedule):
            raise ValueError(
                f"When providing a `{setting_name}` schedule, each tuple in the "
                f"schedule list must have exctly 2 items of the form "
                f"(`timestep`, `{description} to reach`), for example "
                "`[(0, 0.001), (1e6, 0.0001), (2e6, 0.00005)]`."
            )

    def get_current_value(self) -> TensorType:
        """Returns the current value (as a tensor variable).

        This method should be used in loss functions of other (in-graph) places
        where the current value is needed.

        Returns:
            The tensor variable (holding the current value to be used).
        """
        return self._curr_value

    def update(self, timestep: int) -> float:
        """Updates the underlying (framework specific) tensor variable.

        In case of a fixed value, this method does nothing and only returns the fixed
        value as-is.

        Args:
            timestep: The current timestep that the update might depend on.

        Returns:
            The current value of the tensor variable as a python float.
        """
        if self.use_schedule:
            python_value = self._schedule.value(t=timestep)
            if self.framework == "torch":
                self._curr_value.data = torch.tensor(python_value)
            else:
                self._curr_value.assign(python_value)
        else:
            python_value = self._curr_value

        return python_value

    def _create_tensor_variable(self, initial_value: float) -> TensorType:
        """Creates a framework-specific tensor variable to be scheduled.

        Args:
            initial_value: The initial (float) value for the variable to hold.

        Returns:
            The created framework-specific tensor variable.
        """
        if self.framework == "torch":
            return torch.tensor(
                initial_value,
                requires_grad=False,
                dtype=torch.float32,
                device=self.device,
            )
        else:
            return tf.Variable(
                initial_value,
                trainable=False,
                dtype=tf.float32,
            )
