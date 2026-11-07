# Source:
# https://github.com/kubernetes-client/python/blob/master/kubernetes/utils/quantity.py
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

from ray._private.accelerators.tpu import (
    GKE_TPU_ACCELERATOR_TO_GENERATION,
    TPUAcceleratorManager,
    infer_tpu_pod_type_from_topology,
)

# Mapping used to get generation for TPU-{accelerator}-head resource
# https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#run
gke_tpu_accelerator_to_generation = GKE_TPU_ACCELERATOR_TO_GENERATION


def parse_quantity(quantity: Union[str, int, float, Decimal]) -> Decimal:
    """Parse kubernetes canonical form quantity like 200Mi to a decimal number.
    Supported SI suffixes:
    base1024: Ki | Mi | Gi | Ti | Pi | Ei
    base1000: n | u | m | "" | k | M | G | T | P | E

    See
    https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go

    Args:
        quantity: kubernetes canonical form quantity.

    Returns:
        The parsed quantity as a decimal number.

    Raises:
        ValueError: On invalid or unknown input
    """
    if isinstance(quantity, (int, float, Decimal)):
        return Decimal(quantity)

    exponents = {
        "n": -3,
        "u": -2,
        "m": -1,
        "K": 1,
        "k": 1,
        "M": 2,
        "G": 3,
        "T": 4,
        "P": 5,
        "E": 6,
    }

    quantity = str(quantity)
    number = quantity
    suffix = None
    if len(quantity) >= 2 and quantity[-1] == "i":
        if quantity[-2] in exponents:
            number = quantity[:-2]
            suffix = quantity[-2:]
    elif len(quantity) >= 1 and quantity[-1] in exponents:
        number = quantity[:-1]
        suffix = quantity[-1:]

    try:
        number = Decimal(number)
    except InvalidOperation:
        raise ValueError("Invalid number format: {}".format(number))

    if suffix is None:
        return number

    if suffix.endswith("i"):
        base = 1024
    elif len(suffix) == 1:
        base = 1000
    else:
        raise ValueError("{} has unknown suffix".format(quantity))

    # handle SI inconsistency
    if suffix == "ki":
        raise ValueError("{} has unknown suffix".format(quantity))

    if suffix[0] not in exponents:
        raise ValueError("{} has unknown suffix".format(quantity))

    exponent = Decimal(exponents[suffix[0]])
    return number * (base**exponent)


def tpu_node_selectors_to_type(topology: str, accelerator: str) -> Optional[str]:
    """Convert Kubernetes gke-tpu nodeSelectors to TPU accelerator_type
    for a kuberay TPU worker group.
    Args:
        topology: value of the cloud.google.com/gke-tpu-topology Kubernetes
            nodeSelector, describes the physical topology of the TPU podslice.
        accelerator: value of the cloud.google.com/gke-tpu-accelerator nodeSelector,
            the name of the TPU accelerator, e.g. tpu-v4-podslice
    Returns:
        A string, accelerator_type, e.g. "v4-8".
    """
    if topology and accelerator:
        generation = gke_tpu_accelerator_to_generation.get(accelerator)
        normalized_topology = topology.strip().lower()
        if generation and TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
            generation, normalized_topology
        ):
            return infer_tpu_pod_type_from_topology(normalized_topology, generation)
    return None
