Core API
========

.. autosummary::
    :toctree: api/

    ray.init
    ray.shutdown
    ray.is_initialized

Task
----

.. autosummary::
    :toctree: api/

    ray.remote
    ray.remote_function.RemoteFunction.options
    ray.cancel

Actor
-----

.. autosummary::
    :toctree: api/

    ray.remote
    ray.actor.ActorClass.options
    ray.method
    ray.get_actor
    ray.kill

Object
------

.. autosummary::
    :toctree: api/

    ray.get
    ray.wait
    ray.put

.. _runtime-context-apis:

Runtime Context
---------------
.. autosummary::
    :toctree: api/

    ray.runtime_context.get_runtime_context
    ray.runtime_context.RuntimeContext
    ray.get_gpu_ids

Cross Language
--------------
.. autosummary::
    :toctree: api/

    ray.cross_language.java_function
    ray.cross_language.java_actor_class
