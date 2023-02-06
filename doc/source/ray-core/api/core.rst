Core API
========

.. autosummary::
    :toctree: doc/

    ray.init
    ray.shutdown
    ray.is_initialized

Task
----

.. autosummary::
    :toctree: doc/

    ray.remote
    ray.remote_function.RemoteFunction.options
    ray.cancel

Actor
-----

.. autosummary::
    :toctree: doc/

    ray.remote
    ray.actor.ActorClass.options
    ray.method
    ray.get_actor
    ray.kill

Object
------

.. autosummary::
    :toctree: doc/

    ray.get
    ray.wait
    ray.put

.. _runtime-context-apis:

Runtime Context
---------------
.. autosummary::
    :toctree: doc/

    ray.runtime_context.get_runtime_context
    ray.runtime_context.RuntimeContext
    ray.get_gpu_ids
