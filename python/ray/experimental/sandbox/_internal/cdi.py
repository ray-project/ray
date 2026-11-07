"""Generates the CDI spec for a Ray accelerator resource, such as "GPU", for
any vendor whose accelerator manager supports CDI.
"""

from typing import Optional

from ray.experimental.sandbox._internal import cdi_lib
from ray.experimental.sandbox.exceptions import SandboxCreationError


def get_spec(resource_name: str) -> Optional[cdi_lib.CDISpec]:
    """Generate the CDI spec for the accelerator currently resolved for
    `resource_name` (e.g. "GPU" -> whichever of NVIDIA/AMD/Apple/Metax is
    on this node).

    Args:
        resource_name: The Ray resource name to resolve an accelerator
            manager for.

    Returns:
        A `cdi_lib.CDISpec`, or None if there's no CDI-capable accelerator
        manager for `resource_name` on this node.

    Raises:
        RuntimeError: If the manager's `generate_cdi_spec` fails (e.g.
            NVIDIA's, when nvidia-ctk is missing or too old).
    """
    from ray._private.accelerators import get_accelerator_manager_for_resource

    manager = get_accelerator_manager_for_resource(resource_name)
    if manager is None:
        return None
    kind = manager.get_cdi_kind()
    if kind is None:
        return None
    return cdi_lib.CDISpec.generate(kind, manager.generate_cdi_spec)


def require_spec(resource_name: str) -> cdi_lib.CDISpec:
    """Generate the CDI spec for `resource_name`, as `get_spec` does.

    Args:
        resource_name: The Ray resource name to resolve an accelerator
            manager for.

    Returns:
        The `cdi_lib.CDISpec` that `get_spec` returns.

    Raises:
        SandboxCreationError: If generating the spec fails, or this node has
            no CDI spec for `resource_name`.
    """
    try:
        spec = get_spec(resource_name)
    except RuntimeError as err:
        raise SandboxCreationError(str(err)) from err
    if spec is None:
        raise SandboxCreationError(
            f"No CDI spec could be generated for this node's {resource_name} "
            "accelerator, since no accelerator manager on it supports CDI."
        )
    return spec
