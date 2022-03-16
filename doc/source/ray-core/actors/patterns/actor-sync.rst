Pattern: Multi-node synchronization using an Actor
==================================================

When you have multiple tasks that need to wait on some condition or otherwise
need to synchronize across tasks & actors on a cluster, you can use a central
actor to coordinate among them. Below is an example of using a ``SignalActor``
that wraps an ``asyncio.Event`` for basic synchronization.

Code example
------------

.. literalinclude:: ../../doc_code/actor-sync.py
