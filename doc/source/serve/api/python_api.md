# Python API

(core-apis)=

```{eval-rst}
.. currentmodule:: ray
```

## Core APIs

```{eval-rst}
.. class:: serve.Application

.. autoclass:: serve.Deployment
   :members: bind, options, set_options, func_or_class

.. autosummary::
   :toctree: doc/

   serve.run
   serve.start
   serve.shutdown
   serve.delete
```

(servehandle-api)=
## ServeHandle API

```{eval-rst}
.. autosummary::
   :toctree: doc/

   serve.handle.RayServeHandle

.. autosummary::
   :toctree: doc/

   serve.handle.RayServeHandle.remote
   serve.handle.RayServeHandle.options
```

## Batching Requests

```{eval-rst}
.. autosummary::
   :toctree: doc/

   serve.batch
```

## Deployment Graph APIs

```{eval-rst}
.. autosummary::
   :toctree: doc/

   serve.build
   serve.BuiltApplication
```
