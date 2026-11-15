---
myst:
  html_meta:
    description: "API reference for the ray.serve functions that start Serve, run and delete applications, check status, and shut Serve down."
---

(core-apis)=

# Running applications

These `ray.serve` functions start Serve on a Ray cluster, deploy and delete applications, report status, and shut Serve down.

```{eval-rst}
.. currentmodule:: ray
```

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.start
   serve.run
   serve.delete
   serve.status
   serve.shutdown
   serve.shutdown_async
```
