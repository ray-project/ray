import dask
from packaging.version import Version

# Version(dask.__version__) becomes "0" during doc builds.
if Version(dask.__version__) != Version("0") and Version(dask.__version__) < Version(
    "2024.11.0"
):
    # Dask on Ray doesn't work if Dask version is less than 2024.11.0.
    raise ImportError(
        "Dask on Ray requires Dask version 2024.11.0 or later. "
        "Please upgrade your Dask installation."
    )

from .callbacks import (
    ProgressBarCallback,
    RayDaskCallback,
    local_ray_callbacks,
    unpack_ray_callbacks,
)
from .optimizations import dataframe_optimize
from .scheduler import (
    disable_dask_on_ray,
    enable_dask_on_ray,
    ray_dask_get,
    ray_dask_get_sync,
)

dask_persist = dask.persist


def ray_dask_persist(*args, **kwargs):
    kwargs["ray_persist"] = True
    return dask_persist(*args, **kwargs)


ray_dask_persist.__doc__ = dask_persist.__doc__

dask_persist_mixin = dask.base.DaskMethodsMixin.persist


def ray_dask_persist_mixin(self, **kwargs):
    kwargs["ray_persist"] = True
    return dask_persist_mixin(self, **kwargs)


ray_dask_persist_mixin.__doc__ = dask_persist_mixin.__doc__


# We patch dask in order to inject a kwarg into its `dask.persist()` calls,
# which the Dask-on-Ray scheduler needs.
# FIXME(Clark): Monkey patching is bad and we should try to avoid this.
def patch_dask(ray_dask_persist, ray_dask_persist_mixin):
    dask.persist = ray_dask_persist
    dask.base.DaskMethodsMixin.persist = ray_dask_persist_mixin


patch_dask(ray_dask_persist, ray_dask_persist_mixin)

__all__ = [
    # Config
    "enable_dask_on_ray",
    "disable_dask_on_ray",
    # Schedulers
    "ray_dask_get",
    "ray_dask_get_sync",
    # Helpers
    "ray_dask_persist",
    # Callbacks
    "RayDaskCallback",
    "local_ray_callbacks",
    "unpack_ray_callbacks",
    # Optimizations
    "dataframe_optimize",
    "ProgressBarCallback",
]
