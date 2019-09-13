Fault Tolerance
===============

This document describes the handling of failures in Ray.

Machine and Process Failures
----------------------------

Each **raylet** (the scheduler process) sends heartbeats to a **monitor**
process. If the monitor does not receive any heartbeats from a given raylet for
some period of time (about ten seconds), then it will mark that process as dead.

Lost Objects
------------

If an object is needed but is lost or was never created, then the task that
created the object will be re-executed to create the object. If necessary, tasks
needed to create the input arguments to the task being re-executed will also be
re-executed. This is the standard *lineage-based fault tolerance* strategy used
by other systems like Spark.

Actors
------

When an actor dies (either because the actor process crashed or because the node
that the actor was on died), by default any attempt to get an object from that
actor that cannot be created will raise an exception. Subsequent releases will
include an option for automatically restarting actors.

Current Limitations
-------------------

At the moment, Ray does not handle all failure scenarios. We are working on
addressing these known problems.

Process Failures
~~~~~~~~~~~~~~~~

1. Ray does not recover from the failure of any of the following processes:
   any of the Redis servers and the monitor process.
2. If a driver fails, that driver will not be restarted and the job will not
   complete.

Lost Objects
~~~~~~~~~~~~

1. If an object is constructed by a call to ``ray.put`` on the driver, is then
   evicted, and is later needed, Ray will not reconstruct this object.
2. If an object is constructed by an actor method, is then evicted, and is later
   needed, Ray will not reconstruct this object.
