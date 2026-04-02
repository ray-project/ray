.. _placement-group-lifecycle:

Placement Groups
================

This doc will talk about the lifecycle of a placement group in Ray Core, such as how placement groups are created, the two-phase commit schedule, and execution.First of all, what is a placement group? It is a way to schedule a group of bundles of resources atomically across some number of nodes. For more details its better to first read up on the outside docs for `Placement Groups <https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html>`__
The following code is an example of calling a placement group with internals based on Ray 2.54.

.. testcode::

    import ray

    from ray.util.placement_group import (
        placement_group,
        placement_group_table,
        remove_placement_group,
    )

    pg = ray.util.placement_group(
        bundles = [{"CPU": 2, "GPU": 4}] * 16, 
        bundle_label_selector = [{"ray.io/accelerator-type": "GB300"}] * 16, label_locality = {"rack_id" : "STRICT_PACK"},
    )

    ray.get(pg.ready(), timeout=10)
    print(placement_group_table(pg))

.. testoutput::

   
