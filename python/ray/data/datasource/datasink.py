"""Re-export from internal implementation."""

from ray.data._internal.datasource.datasink import *  # noqa: F401, F403
from ray.data._internal.datasource.datasink import (  # noqa: F401
    _gen_datasink_write_result,
)
