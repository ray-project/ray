from typing import Dict, Optional

import ray
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2.api.config import ScalingConfig
from ray.train.v2.jax.tpu_utils import reserve_tpu_slice


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
            A dictionary defining a `bundle_label_selector` to gang schedule
            the worker group on the reserved TPU slice.
        """
        bundle_label_selector = None

        if getattr(scaling_config, "use_tpu", False) and num_workers > 1:
            slice_name = reserve_tpu_slice(
                topology=getattr(scaling_config, "topology", None),
                accelerator_type=getattr(scaling_config, "accelerator_type", None),
            )
            if not slice_name:
                raise RuntimeError("Failed to reserve TPU slice.")

            bundle_label_selector = {
                ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name
            }

        return bundle_label_selector
