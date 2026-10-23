Core API
========

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.init
    ray.shutdown
    ray.is_initialized
    ray.job_config.JobConfig
    ray.LoggingConfig

Tasks
-----

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.remote
    ray.remote_function.RemoteFunction.options
    ray.cancel

Actors
------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.remote
    ray.actor.ActorClass
    ray.actor.ActorClass.options
    ray.actor.ActorMethod
    ray.actor.ActorHandle
    ray.actor.ActorClassInheritanceException
    ray.actor.exit_actor
    ray.method
    ray.get_actor
    ray.kill

Objects
-------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.get
    ray.wait
    ray.put
    ray.util.as_completed
    ray.util.map_unordered

.. _runtime-context-apis:

Runtime Context
---------------
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.runtime_context.get_runtime_context
    ray.runtime_context.RuntimeContext
    ray.get_gpu_ids

Cross Language
--------------
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.cross_language.java_function
    ray.cross_language.java_actor_class
