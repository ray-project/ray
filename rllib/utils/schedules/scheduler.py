from collections import defaultdict
from typing import List, Optional, Tuple

from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.schedules.piecewise_schedule import PiecewiseSchedule
from ray.rllib.utils.typing import TensorType


_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class Scheduler:
    """Class to manage a scheduled (framework-dependent) tensor variable.

    Uses the PiecewiseSchedule (for maximum configuration flexibility)
    """

    def __init__(
        self,
        *,
        fixed_value: Optional[float] = None,
        schedule: Optional[List[Tuple[int, float]]] = None,
        framework: str = "torch",
        device: Optional[str] = None,
    ):
        """Initializes a Scheduler instance.

        Args:
            fixed_value: A fixed, constant value (in case no schedule should be used).
                Set `schedule` to None to always just use this fixed value.
                If `fixed_value` is None, `schedule` must be provided.
            schedule: The schedule configuration to use. In the format of
                [[timestep, value], [timestep, value], ...]
                Intermediary timesteps will be assigned to interpolated values (linear
                interpolation will be used). A schedule config's first entry must
                start with timestep 0, i.e.: [[0, initial_value], [...]].
            framework: The framework string, for which to create the tensor variable
                that hold the current value. This is the variable that can be used in
                the graph, e.g. in a loss function.
            device: Optional device (for torch) to place the tensor variable on.
        """
        self.use_schedule = schedule is not None
        self.framework = framework
        self.device = device

        if self.use_schedule:
            # Custom schedule, based on list of
            # ([ts], [value to be reached by ts])-tuples.
            self.schedule_per_module = defaultdict(
                lambda: PiecewiseSchedule(
                    schedule,
                    outside_value=schedule[-1][-1],
                    framework=None,
                )
            )
            # As initial tensor valie, use the first timestep's (must be 0) value.
            self.curr_value_per_module = defaultdict(
                lambda: self._create_tensor_variable(initial_value=schedule[0][1])
            )
        # If no schedule, pin (fix) given value.
        else:
            self.curr_value_per_module = defaultdict(lambda: fixed_value)

    @staticmethod
    def validate(
        schedule: Optional[List[Tuple[int, float]]],
        schedule_name: str,
        value_name: str,
    ) -> None:
        """Performs checking of a certain schedule configuration.

        The first entry in `schedule` must have a timestep of 0.

        Args:
            schedule: The schedule configuration to check. In the format of
                [[timestep, value], [timestep, value], ...]
                Intermediary timesteps will be assigned to interpolated values (linear
                interpolation will be used). A schedule config's first entry must
                start with timestep 0, i.e.: [[0, initial_value], [...]].
            schedule_name: The name of the schedule, e.g. `lr_schedule`.
            value_name: A full text description of the variable that's being scheduled,
                e.g. `learning rate`.

        Raises:
            ValueError: In case, errors are found in the schedule's format.
        """
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

    def get_current_value(self, module_id: ModuleID) -> TensorType:
        """Returns the current value (as a tensor variable), given a ModuleID.

        Args:
            module_id: The module ID, for which to retrueve the current tensor value.

        Returns:
            The tensor variable (holding the current value to be used).
        """
        return self.curr_value_per_module[module_id]

    def update(self, module_id: ModuleID, timestep: int) -> float:
        """Updates the underlying (framework specific) tensor variable.

        Args:
            module_id: The module ID, for which to update the tensor variable.
            timestep: The current timestep.

        Returns:
            The current value of the tensor variable as a python float.
        """
        if self.use_schedule:
            python_value = self.schedule_per_module[module_id].value(t=timestep)
            if self.framework == "torch":
                self.curr_value_per_module[module_id].data = torch.tensor(python_value)
            else:
                self.curr_value_per_module[module_id].assign(python_value)
        else:
            python_value = self.curr_value_per_module[module_id]

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
