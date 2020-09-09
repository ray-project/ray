from .dynamic_resources import set_resource
from .object_spilling import force_spill_objects, force_restore_spilled_objects
__all__ = [
    "set_resource",
    "force_spill_objects",
    "force_restore_spilled_objects",
]
