from ray._private.accelerators.nvidia_gpu import NvidiaGPUAccelerator
from ray._private.accelerators.tpu import TPUAccelerator
from ray._private.accelerators.neuron import NeuronAccelerator

ALL_ACCELERATORS = {
    NvidiaGPUAccelerator.get_resource_name(): NvidiaGPUAccelerator,
    TPUAccelerator.get_resource_name(): TPUAccelerator,
    NeuronAccelerator.get_resource_name(): NeuronAccelerator,
}

__all__ = [
    "NvidiaGPUAccelerator",
    "TPUAccelerator",
    "NeuronAccelerator",
    "ALL_ACCELERATORS",
]
