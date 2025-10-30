import ray
from settings import RAY_NAMESPACE, REGISTRY_NAME


class WorkerWrap:  # vulture: ignore
    """Extension injected into every vLLM worker process.

    The extension exposes helpers that the generator calls via
    `engine.collective_rpc``
    """

    def update_weights(self):
        """Load CUDA tensors directly into the model.

        Receives weight tensors via GPU-to-GPU transfer (NIXL) from the learner.
        Each weight tensor has shape specific to the layer (e.g., [HIDDEN_DIM, HIDDEN_DIM] for weights).
        """
        registry_handle = ray.get_actor(REGISTRY_NAME, namespace=RAY_NAMESPACE)

        list_of_weight_refs = ray.get(
            registry_handle.get.remote()
        )  # List of ObjectRefs, one per named weight.

        # Transfer weights one by one and use load_weights which handles TP sharding correctly.
        # TODO: this could be sped up by updating more weights at once, or calling load_weights asynchronously.
        for weight_ref in list_of_weight_refs:
            payload: tuple = ray.get(weight_ref)

            # load_weights expects a list of (name, weight) pairs
            self.model_runner.model.load_weights(weights=[payload])
            del payload
