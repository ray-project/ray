from typing import Dict, Optional

import ray
from ray._private.accelerators.tpu import reserve_tpu_slice
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2.api.config import ScalingConfig


class TPUReservationCallback(ControllerCallback):
    """A callback to handle TPU slice reservation for multi-host training."""

    def on_controller_start_worker_group(
        self, *, scaling_config: ScalingConfig, num_workers: int
    ) -> Optional[Dict[str, str]]:
        """Reserves a multi-host TPU slice before the worker group starts.

        This hook is called by the TrainController. It checks if multi-host
        TPUs are being used and, if so, reserves a slice.

        Args:
            scaling_config: The scaling configuration for the run.
            num_workers: The number of workers to be started.

        Returns:
            A dictionary defining a `label_selector` to gang schedule
            the worker group on the reserved TPU slice.
        """
        label_selector = None

        if scaling_config.use_tpu and num_workers > 1:
            assert scaling_config.accelerator_type is not None
            assert scaling_config.topology is not None

            slice_name = reserve_tpu_slice(
                topology=scaling_config.topology,
                accelerator_type=scaling_config.accelerator_type,
            )
            if not slice_name:
                raise RuntimeError("Failed to reserve TPU slice.")

            label_selector = {ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name}

        return label_selector
