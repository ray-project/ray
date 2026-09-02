"""Generates the CDI spec for a Ray accelerator resource (e.g. "GPU"),
generic across vendors via `ray._private.accelerators.get_accelerator_manager_for_resource`.

Lives in `_common`, not inlined at each call site: libraries (e.g.
`ray.experimental.sandbox`) must not import `ray._private` directly, so
this is the layering boundary that lets them reach CDI generation without
doing so themselves.
"""

from typing import Optional

from ray._common import cdi_lib


def get_spec(resource_name: str) -> Optional[cdi_lib.CDISpec]:
    """Generate (or return the cached) CDI spec for the accelerator
    currently resolved for `resource_name` (e.g. "GPU" -> whichever of
    NVIDIA/AMD/Apple/Metax is actually on this node).

    Args:
        resource_name: The Ray resource name to resolve an accelerator
            manager for.

    Returns:
        A `cdi_lib.CDISpec`, or None if there's no CDI-capable accelerator
        manager for `resource_name` on this node, or generation failed.
    """
    from ray._private.accelerators import get_accelerator_manager_for_resource

    manager = get_accelerator_manager_for_resource(resource_name)
    if manager is None:
        return None
    kind = manager.get_cdi_kind()
    if kind is None:
        return None
    return cdi_lib.CDISpec.generate(kind, manager.generate_cdi_spec)
