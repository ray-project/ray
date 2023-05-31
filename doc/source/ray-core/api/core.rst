Core API
========

.. autosummary::
    :toctree: doc/

    ray.init
    ray.shutdown
    ray.is_initialized
    ray.job_config.JobConfig

Tasks
-----

.. autosummary::
    :toctree: doc/

    ray.remote
    ray.remote_function.RemoteFunction.options
    ray.cancel

Actors
------

.. autosummary::
    :toctree: doc/

    ray.remote
    ray.actor.ActorClass.options
    ray.method
    ray.get_actor
    ray.kill

Objects
-------

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

Cross Language
--------------
.. autosummary::
    :toctree: doc/

    ray.cross_language.java_function
    ray.cross_language.java_actor_class
