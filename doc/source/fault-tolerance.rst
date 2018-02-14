Fault Tolerance
===============

This document describes the handling of failures in Ray.

Machine and Process Failures
----------------------------

Currently, each **local scheduler** and each **plasma manager** send heartbeats
to a **monitor** process. If the monitor does not receive any heartbeats from a
given process for some duration of time (about ten seconds), then it will mark
that process as dead. The monitor process will then clean up the associated
state in the Redis servers. If a manager is marked as dead, the object table
will be updated to remove all occurrences of that manager so that other managers
don't try to fetch objects from the dead manager. If a local scheduler is marked
as dead, all of the tasks that are marked as executing on that local scheduler
in the task table will be marked as lost and all actors associated with that
local scheduler will be recreated by other local schedulers.

Lost Objects
------------

If an object is needed but is lost or was never created, then the task that
created the object will be re-executed to create the object. If necessary, tasks
needed to create the input arguments to the task being re-executed will also be
re-executed.

Actors
------

When a local scheduler is marked as dead, all actors associated with that local
scheduler that were still alive will be recreated by other local schedulers. By
default, all of the actor methods will be re-executed in the same order that
they were initially executed. If actor checkpointing is enabled, then the actor
state will be loaded from the most recent checkpoint and the actor methods that
occurred after the checkpoint will be re-executed. Note that actor checkpointing
is currently an experimental feature.


Current Limitations
-------------------

At the moment, Ray does not handle all failure scenarios. We are working on
addressing these known problems.

Process Failures
~~~~~~~~~~~~~~~~

1. Ray does not recover from the failure of any of the following processes:
   a Redis server, the global scheduler, the monitor process.
2. If a driver fails, that driver will not be restarted and the job will not
   complete.

Lost Objects
~~~~~~~~~~~~

1. If an object is constructed by a call to ``ray.put`` on the driver, is then
   evicted, and is later needed, Ray will not reconstruct this object.
2. If an object is constructed by an actor method, is then evicted, and is later
   needed, Ray will not reconstruct this object.

Actor Reconstruction
~~~~~~~~~~~~~~~~~~~~

1. Actor reconstruction follows the order of initial execution, but new tasks
   may get interleaved with the re-executed tasks.
