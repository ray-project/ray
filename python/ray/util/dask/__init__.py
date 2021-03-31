import dask
from .scheduler import ray_dask_get, ray_dask_get_sync
from .callbacks import (
    RayDaskCallback,
    local_ray_callbacks,
    unpack_ray_callbacks,
)
from .optimizations import dataframe_optimize

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
]
