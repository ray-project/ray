GCS-Based Actor Scheduling
================

.. _ray-gcs-based-actor-scheduling-doc-ref:

When an actor is created with the (state-of-the-art) raylet-based actor scheduler, we rely on a raylet (where the actor's owner locates, 
or randomly selected) to make a placement decision based on its local view of the cluster resources. Because there is not a unified 
resource view across raylets, this actor scheduling might be unbalanced (overly packing at a few nodes) and inefficient (placement conflicts).

GCS-based actor scheduling is expected to resolve these issues. With a unified and fresh resource view, GCS, as a centralized scheduler, is able
to schedule actors without any conflicts. Any scheduling strategies (e.g., evenly spread for load balancing, packing for less fragmentation)
can be also strictly enforced.


How to turn on the GCS-based actor scheduling
------------
Set environment variable ``RAY_gcs_actor_scheduling_enabled`` to ``true`` to turn on the GCS-based actor scheduling. For now, 
this environment variable is set to ``false`` by default, which means the conventional Raylet-based actor scheduling is used.
