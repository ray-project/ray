"""
Constants.

Contains constants used to setup collective groups.
"""
import hashlib
import os
from enum import Enum, auto


def get_store_name(group_name):
    """Generate the unique name for the NCCLUniqueID store (named actor).

    Args:
        group_name: unique user name for the store.
    Return:
        str: SHA1-hexlified name for the store.
    """
    if not group_name:
        raise ValueError("group_name is None.")
    hexlified_name = hashlib.sha1(group_name.encode()).hexdigest()
    return hexlified_name


class ENV(Enum):
    """ray.util.collective environment variables."""

    NCCL_USE_MULTISTREAM = auto(), lambda v: (v or "True") == "True"

    @property
    def val(self):
        """Return the output of the lambda against the system's env value."""
        _, default_fn = self.value
        return default_fn(os.getenv(self.name))
