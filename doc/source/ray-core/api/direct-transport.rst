Ray Direct Transport (RDT) API
==============================

Usage with Core APIs
--------------------
Enable RDT for actor tasks with the :func:`@ray.method <ray.method>` decorator, or pass `_tensor_transport` to :func:`ray.put`. You can then pass the resulting `ray.ObjectRef` to other actor tasks, or use :func:`ray.get` to retrieve the result. See :ref:`Ray Direct Transport (RDT) <direct-transport>` for more details on usage.


.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.method
    ray.put
    ray.get

Collective tensor transports
----------------------------
Collective tensor transports require a collective group to be created before RDT objects can be used. Use these methods to create and manage collective groups for the `gloo` and `nccl` tensor transports.


.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.experimental.collective.create_collective_group
    ray.experimental.collective.get_collective_groups
    ray.experimental.collective.destroy_collective_group