Pattern: Using an actor to synchronize other tasks and actors
=============================================================

When you have multiple tasks that need to wait on some condition or otherwise
need to synchronize across tasks & actors on a cluster, you can use a central
actor to coordinate among them.

Example use case
----------------

You can use an actor to implement a distributed ``asyncio.Event`` that multiple tasks can wait on.

Code example
------------

.. literalinclude:: ../doc_code/actor-sync.py
