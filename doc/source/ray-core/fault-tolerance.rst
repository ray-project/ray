Fault tolerance
===============

Ray is a distributed system, and a lot of failures can happen. Generally, it can
be classified into two classes: 1) failure because of ray internally; 2) failure
due to the applications. The first is likely to be triggered by node failure,
network failure, or just bugs in Ray. The latter could happen because the user
has bugs in their code or some external systems fail, and the failure is not
handled well by the code. 
Ray has built a lot of mechanisms to do its best to recover from the failure in
both cases to ensure the distributed apps are fault tolerant.

.. _fault-tolerance_ownership:

Ownership model
---------------

To understand the failure model of Ray, we need to understand the ownership
model of Ray first. The owner of an object or non-detached actor is defined as
the worker who creates it, and the detached actor’s owner is always GCS.
Consider the following example: 

.. code-block:: python
    
    @ray.remote
    class Actor:
        pass


    @ray.remote
    def foo():
        obj = ray.put(10)
        actor = Actor.remote()
        actor_detached = Actor.options(lifetime=”detached”).remote()
        return (obj, actor, actor_detached)

    # the driver owns obj1 since the driver created it
    obj1 = ray.put(10)

    # obj2 is owned by the driver since it’s also the driver who created it. 
    obj2 = foo.remote()

    # obj3 and actor1 are owned by the worker where foo.remote() is executed.
    # actor2 is owned by the worker since it’s a detached actor.
    (obj3, actor1, actor2) = ray.get(obj2)


Here, ``obj1`` and ``obj2`` are owned by the driver since this is the place
where they are created.  ``obj3`` and ``actor1`` are owned by the worker where
foo is scheduled to run since they are created there. As for ``actor2``,
although it’s created in the same worker as ``obj3`` and ``actor1``, the  owner
is actually the GCS since it’s a detached actor. Owners are the places where all
metadata of the resources are stored. The owner is defined as dead if the worker
process exits. This could happen if the raylet which owns the worker died or the
next tasks scheduled on the worker crashed the worker, or it’s just been killed
by the OS or the user. 

Fault tolerance in Ray
----------------------

.. toctree::
    :maxdepth: 1

    fault_tolerance/objects.rst
    fault_tolerance/tasks.rst
    fault_tolerance/actors.rst
    fault_tolerance/ray_components.rst
