.. _object-fault-tolerance:

Fault Tolerance
===============

Typical use of objects is covered by :ref:`task <task-fault-tolerance>` and :ref:`actor <actor-fault-tolerance>` fault tolerance. This section covers more advanced fault tolerance situations that may occur if passing objects by reference around in a Ray application.

Object Ownership and Fault Tolerance
------------------------------------

Ray delegates the metadata tracking of an object to its *owner process*. Typically, the owner of an object is the worker that created it via ``ray.put()``, or the worker that submitted the task generating an object. For example, the owner of an object could be an actor or the driver process.

The owner of the object tracks the location and reference count for an object. If the owner process is unexpectedly killed, then the object cannot be recovered, even via lineage reconstruction.

For more information about how object ownership works, see the :ref:`Ray Architecture Whitepaper <whitepaper>`.
