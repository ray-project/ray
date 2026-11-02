---
myst:
  html_meta:
    description: "API reference for writing Ray Serve applications: the Deployment and Application classes and the deployment, ingress, batch, and multiplexed decorators in ray.serve."
---

(serve-api-application)=

# Writing applications

These `ray.serve` APIs define deployments and compose them into applications.

```{eval-rst}
.. currentmodule:: ray
```

## Deployments and applications

<!---
NOTE: `serve.deployment` and `serve.Deployment` have an autosummary-generated filename collision due to case insensitivity. This is fixed by added custom filename mappings in `source/conf.py` (look for "autosummary_filename_map"). --->

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_init_args.rst

   serve.Deployment
   serve.Application
```

## Deployment decorators

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.deployment
      :noindex:
   serve.ingress
   serve.batch
   serve.multiplexed
```
