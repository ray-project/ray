---
myst:
  html_meta:
    description: "API reference for Dask on Ray scheduler callbacks: RayDaskCallback and its hook methods."
---

(dask-on-ray-api-ref)=

# Dask on Ray API

For usage, see {doc}`/ray-more-libs/dask-on-ray`.

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   ~ray.util.dask.RayDaskCallback
   ~ray.util.dask.callbacks.RayDaskCallback._ray_presubmit
   ~ray.util.dask.callbacks.RayDaskCallback._ray_postsubmit
   ~ray.util.dask.callbacks.RayDaskCallback._ray_pretask
   ~ray.util.dask.callbacks.RayDaskCallback._ray_posttask
   ~ray.util.dask.callbacks.RayDaskCallback._ray_postsubmit_all
   ~ray.util.dask.callbacks.RayDaskCallback._ray_finish
```
