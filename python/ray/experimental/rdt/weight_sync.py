import logging
import threading
import time
from typing import Any, Dict, List, Union

import ray
from ray._raylet import ObjectRef
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class WeightSyncManager:
    """Manager for orchestrating Ray-native weight updates using RDT (Ray Direct Transport).

    This manager provides high-performance GPU-to-GPU weight updates by accepting
    Ray ObjectRefs directly and using direct GPU transfers under the hood.

    Thread Safety:
        This class is safe for use in Ray actors with ``max_concurrency > 1``.
        A standard lock serializes the version check and weight-load critical
        section, preventing TOCTOU races that could regress the model to stale
        weights when concurrent calls arrive out of order.
    """

    def __init__(self, model: Any):
        """Initialize the weight synchronization manager.

        Args:
            model: The PyTorch neural network model whose weights will be updated.
        """
        self.model = model
        self.current_version = -1
        # Protects the version check → weight load → version commit critical section
        # against TOCTOU races in actors running with max_concurrency > 1.
        self._lock = threading.Lock()

    def update_weights_from_object_ref(
        self,
        weight_refs: Union[ObjectRef, List[ObjectRef]],
        version: int,
    ) -> Dict[str, Any]:
        """Update model weights from one or more Ray ObjectRefs.

        The version check and the weight commit are performed under a single
        lock acquisition so that concurrent callers cannot both pass the guard
        and then apply their updates out of order.

        Args:
            weight_refs: A single ObjectRef or a list of ObjectRefs containing
                state dict chunks.
            version: The monotonically increasing version number of the weights.

        Returns:
            A dictionary containing status metrics of the synchronization.

        Raises:
            ValueError: If ``version`` is not strictly greater than
                ``current_version``, or if ``weight_refs`` is empty.
            TypeError: If chunked refs do not contain ``dict`` state weights.
            AttributeError: If the model exposes neither ``load_state_dict``
                nor ``load_weights``.
        """
        start_time = time.time()

        if version <= self.current_version:
            raise ValueError(
                f"Weight version must be monotonically increasing. "
                f"Received version {version}, current version is "
                f"{self.current_version}"
            )

        # 1. Normalize monolithic vs chunked refs *before* acquiring the lock
        #    so that I/O-bound work (ray.get) does not hold the lock.
        is_chunked = not isinstance(weight_refs, ObjectRef)
        ref_list = list(weight_refs) if is_chunked else [weight_refs]

        if not ref_list:
            raise ValueError("weight_refs cannot be empty.")

        # 2. Retrieve tensors via Ray Direct Transport in parallel.
        #    ray.get is intentionally called *outside* the lock so that
        #    multiple callers can fetch their object data concurrently; only
        #    the model mutation is serialised.
        try:
            # Under the hood, RDT transparently bypasses the Plasma store for
            # GPU tensors, enabling direct GPU-to-GPU transfers.
            loaded_chunks = ray.get(ref_list)
        except Exception as e:
            logger.error(f"Failed to fetch weight refs for version {version}: {e}")
            raise

        # 3. Consolidate chunks into a single state dict before the lock.
        if not is_chunked:
            state_dict = loaded_chunks[0]
            if not isinstance(state_dict, dict):
                raise TypeError("Weight ref must contain a dict of state weights.")
        else:
            state_dict = {}
            for chunk in loaded_chunks:
                if isinstance(chunk, dict):
                    state_dict.update(chunk)
                else:
                    raise TypeError(
                        "Chunked weight refs must contain dicts of state weights."
                    )

        # 4. Critical section: version guard + model mutation + version commit.
        #    The lock makes this block atomic, preventing TOCTOU races where
        #    two concurrent callers both pass the version check and then apply
        #    their weights out of order (which would regress the model).
        with self._lock:
            if version <= self.current_version:
                raise ValueError(
                    f"Weight version must be monotonically increasing. "
                    f"Received version {version}, current version is "
                    f"{self.current_version}"
                )

            try:
                # Load the consolidated weights into the model.
                if hasattr(self.model, "load_state_dict"):
                    self.model.load_state_dict(state_dict)
                elif hasattr(self.model, "load_weights"):
                    self.model.load_weights(state_dict.items())
                else:
                    raise AttributeError(
                        "Model does not support load_state_dict or load_weights."
                    )

                # Commit version only after a successful load.
                self.current_version = version

            except Exception as e:
                logger.error(f"Failed to load weights for version {version}: {e}")
                raise

        elapsed = time.time() - start_time
        logger.info(
            f"Successfully updated weights to version {version} in {elapsed:.4f}s."
        )

        return {
            "status": "success",
            "version": version,
            "duration_seconds": elapsed,
            "is_chunked": is_chunked,
            "num_chunks": len(ref_list),
        }
